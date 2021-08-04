"""
Example run: python3 prod_sms_response_time.py --min_prob=0.7 --model=models/response_not_essential_2019-02-28_15-05-44.bin \
    --output_project_id=cityblock-analytics --output_tbl_name=src_communications.sms_response_required_model_output

## Primary Functions

1) Query messages and sessions
2) Determine member state at time of message
3) Predict whether a message ends a conversation

The output ids that are enriched with response and prediction metadata can be right-joined back onto `cityblock-data.commons_mirror.sms_message`

## Prediction
The fastText library is used both to learn word representations and to subsequently classify the messages as being conversation enders, with a focus on precision
i.e. high P(convo-end label | convo-end predicted).  To be conervative, ALL messages in a session must be marked as "conversation-enders" for the session to be
treated as such.

Bias: Training was done on the *final* message of *inbound* sessions only across the whole spectrum of computed response times.

Labels: https://docs.google.com/spreadsheets/d/1COua72MTFjfJplWynsqO-bGrVSFpEUfQEzr4RZ5UPoM/edit#gid=1990083147

## Known issues:
* Older messages (pre-2019) didn't always have a providerCreatedAt, which means they have no session_id
"""

from itertools import groupby
from datetime import datetime, timedelta
import argparse
import uuid

import pandas as pd
import numpy as np
import emoji
from fasttext import load_model

from read import query_db

        
def print_results(N, p, r):

    print("N\t" + str(N))
    print("P@{}\t{:.3f}".format(1, p))
    print("R@{}\t{:.3f}".format(1, r))
    

def custom_pred(row_label, pred_prob, min_thresh):

    if row_label in ['__label__1', '__label__0']:
        
        if row_label == '__label__1':
            prob__label__1 = pred_prob
            if pred_prob >= min_thresh:
                return '__label__1', round(prob__label__1, 3)
            else:
                return '__label__0', round(prob__label__1, 3)
            
        elif row_label == '__label__0':
            prob__label__1 = 1 - pred_prob
            if (1-pred_prob) >= min_thresh:
                return '__label__1', round(prob__label__1, 3)
            else:
                return '__label__0', round(prob__label__1, 3)    
    
    
def cast_columns_to_string(sms_df, cols_to_cast):

    for col in cols_to_cast:
        sms_df[col] = sms_df[col].apply(str)    


def snake_to_camel(snake_str):
    first, *others = snake_str.split('_')
    return ''.join([first.lower(), *map(str.title, others)])
        
        
def get_current_timestamp_str():

    return str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def main(prob__label__1_thresh, model_path, output_project_id, output_tbl_name):

    sms_msg_all = query_db(project_id=output_project_id)

    ## Filter out messages that interfere with time response computation

    # 1) remove voicemail texts
    sms_msg_without_voicemails = (
        sms_msg_all.loc[~sms_msg_all['body'].fillna('').str.startswith('https://commons.cityblock.com/voicemails'), :])
    sms_msg_without_voicemails = (
        sms_msg_without_voicemails.loc[
        ~sms_msg_without_voicemails['body'].fillna('').str.contains('Click the following link to listen to the message'), :]
        )

    # 2) remove [Auto response] texts. Can add these in at end if needed
    sms_msg_without_voicemails_and_auto_response = (
        sms_msg_without_voicemails.loc[
        ~sms_msg_without_voicemails['body'].fillna('').str.startswith('[Auto response]'), :])

    # 3) remove welcome texts
    sms_msg = (
        sms_msg_without_voicemails_and_auto_response.loc[
        ~sms_msg_without_voicemails_and_auto_response['body'].fillna('').str.contains('dialpad.com'), :])

    # Clean noise of identical SMS messages sent in a single session
    sms_msg_sort = sms_msg.drop_duplicates(["session_id", "body"]).sort_values(['body','created_at']).reset_index(drop=True)

    # Attempt to find historical internal phone numbers (no longer assigned to users in Commons)
    print('Mining for historical internal phone numbers to tag')
    last_idx_of_dup = (sms_msg_sort.groupby("body", sort=False)["created_at"].diff().dt.seconds <= 2)

    idx_potential_dups = list(last_idx_of_dup[last_idx_of_dup == True].index) + list(last_idx_of_dup[last_idx_of_dup == True].index - 1)
    idx_potential_dups.sort()
    sms_msg_sort_potential_dups = sms_msg_sort.loc[idx_potential_dups]
    sms_msg_sort_potential_dups['dup_id'] = np.repeat(np.arange(len(idx_potential_dups)//2), 2)

    dup_ids_two_directions = (sms_msg_sort_potential_dups
        .groupby('dup_id')
        .apply(lambda x: all([i in x['direction'].tolist() for i in ['incoming','outgoing']]))
    )
    sms_msg_sort_true_dups = sms_msg_sort_potential_dups[sms_msg_sort_potential_dups['dup_id'].isin(dup_ids_two_directions[dup_ids_two_directions].index)]

    min_internal_dup_occurence = 3
    historical_internal_numbers = (sms_msg_sort_true_dups
        .groupby('contact_number')
        .count()
        .sort_values('id', ascending=False)
        .query(F'id >= {min_internal_dup_occurence}')
        .index
        .values
    )
    
    current_internal_numbers = sms_msg.query('is_internal_contact_number == True')['contact_number'].unique()
    additional_internal_numbers = set(historical_internal_numbers) - set(current_internal_numbers)

    sms_msg.loc[sms_msg['contact_number'].isin(historical_internal_numbers), 'is_internal_contact_number'] = True
    print(F'{len(additional_internal_numbers)} additional internal numbers found')

    # Clean text before vector representation
    sms_msg['body_cleaned'] = (sms_msg['body']
        .fillna('')
        .str.lower()
        .str.replace('?',' ? ') # Enable ?s to be their own token
        .apply(lambda x: emoji.demojize(x)) # Convert emojis to textual descriptions
        .str.replace(r'[^\w\s?]+', ' ') # Remove all punctuation except for ?s
        .str.replace(r'\d', '#') # Replace all digits with #s
        .apply(lambda x: ' '.join(x.split())) # Remove whitespace: multiple spaces, newlines, carriage returns, tabs  # .str.replace(r'\n|\r', ' ') 
    )

    ## Add Predictions to all data

    # Load Fasttext model
    prod_model = load_model(model_path)

    print('Predicting conversation-enders')
    sms_msg['pred_label'] = [prod_model.predict(x)[0][0] for x in sms_msg['body_cleaned']]
    sms_msg['pred_prob'] = [prod_model.predict(x)[1][0] for x in sms_msg['body_cleaned']]
    sms_msg[['custom_pred_label', 'prob__label__1']] = sms_msg.apply(
        lambda row: custom_pred(row['pred_label'], row['pred_prob'], prob__label__1_thresh), axis=1, result_type="expand")

    # We only label responses as unnecessary where **all** messages in the entire session are labelled as unnecessary
    all__label__1 = (sms_msg
                    .query('direction == "incoming"')
                    .groupby(['session_id'])
                    .apply(lambda x: all([i == '__label__1' for i in x['custom_pred_label']])))

    sessions_no_response_needed = all__label__1[all__label__1 == True].index.values
    sms_msg = sms_msg.assign(is_predicted_conversation_end=lambda df: df.session_id.isin(sessions_no_response_needed))

    # Write output to BQ
    print(f'Writing SMS metadata to `{output_project_id}.{output_tbl_name}`')
    output_fields = ['id','session_id','historical_state','coarse_historical_state','is_internal_contact_number',\
                     'is_predicted_conversation_end','prob__label__1','custom_pred_label']

    (sms_msg[output_fields]
        .rename(columns={c: snake_to_camel(c) for c in output_fields if c != 'prob__label__1'})
        .assign(predictionGeneratedAt=get_current_timestamp_str())
        .to_gbq(destination_table=output_tbl_name, project_id=output_project_id, if_exists='replace')
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Model for predicting natural ends to conversation threads')

    parser.add_argument('--min_prob', type=float, help='The probability threshold to deem a message as a conversation ender.  The higher this, the higher the precision', 
                        default=0.7, dest='prob__label__1_thresh')
    parser.add_argument('--model', type=str, help='The path to the fastText .bin model', 
                        dest='model_path', required=True)
    parser.add_argument('--output_project_id', type=str, help='The project to export the metadata', 
                        dest='output_project_id', required=True)
    parser.add_argument('--output_tbl_name', type=str, help='The dataset.table to export the metadata', 
                        dest='output_tbl_name', required=True)
    args = parser.parse_args()

    main(args.prob__label__1_thresh, args.model_path, args.output_project_id, args.output_tbl_name)
