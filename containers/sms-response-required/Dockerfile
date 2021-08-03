FROM nickgryg/alpine-pandas:3.6.8

RUN apk add make automake gcc g++ python3-dev git wget libffi-dev
RUN pip3 install --upgrade pip
RUN pip3 install six==1.15.0
RUN pip3 install Cython==0.29.6 emoji==0.5.1 pandas-gbq==0.13.2 fasttext==0.9.1

WORKDIR /home/$USER/

COPY prod_sms_response_required.py .
COPY read.py .
COPY models/response_not_essential_2019-02-28_15-05-44.bin .

CMD ["python3", \
	 "prod_sms_response_required.py", \
	 "--min_prob=0.7", \
	 "--model=response_not_essential_2019-02-28_15-05-44.bin", \
	 "--output_project_id=cityblock-analytics", \
	 "--output_tbl_name=src_communications.sms_response_required_model_output"]