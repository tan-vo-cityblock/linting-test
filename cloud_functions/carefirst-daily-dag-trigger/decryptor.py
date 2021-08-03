from google.cloud import storage,secretmanager
import tempfile
import pgpy
import io
import pandas as pd

def open_secrets(secret_id):
    client = secretmanager.SecretManagerServiceClient()

    # Access the secret version
    response = client.access_secret_version(name=f"projects/97093835752/secrets/{secret_id}/versions/latest")

    # Return the secret payload
    return response.payload.data.decode('UTF-8')

def download_file(filename,bucket):
    blob = bucket.get_blob(filename)
    encrypted_file = tempfile.NamedTemporaryFile()
    blob.download_to_filename(encrypted_file.name)

    return encrypted_file

def run_decryption(encrypted_file,filename):

    #Get private keys and passcode
    priv_key_txt = open_secrets("carefirst_pgp_private_key")
    passphrase = open_secrets("carefirst_pgp_passcode")

    priv_key = pgpy.PGPKey()
    priv_key.parse(priv_key_txt)
    pass

    #Decrypt file
    with priv_key.unlock(passphrase):
        message_from_file = pgpy.PGPMessage.from_file(encrypted_file.name)
        raw_message = bytes(priv_key.decrypt(message_from_file).message)

    #Decode file properly
    if ".xls" in filename:
        toread = io.BytesIO()
        toread.write(raw_message)
        toread.seek(0)
        df = pd.read_excel(toread)
        decrypted_file = tempfile.NamedTemporaryFile("w+t",suffix=".xlsx")
        try:
            df.to_excel(decrypted_file.name,index=False)
        except Exception as e:
            print(e)
    elif ".txt" in filename:
        decrypted_file = tempfile.NamedTemporaryFile("w+t",suffix=".txt")
        decrypted_file.write(raw_message.decode(encoding="utf-8"))

    decrypted_file.flush()

    #Close temp encrypted file
    encrypted_file.close()

    return decrypted_file

def upload_file(filename,decrypted_file,bucket):
    blob = bucket.blob(filename.replace(".pgp",""))

    blob.upload_from_filename(decrypted_file.name)

    # Close temp decrypted file
    decrypted_file.close()

def decrypt_pgp_file(filename,gcs_client,bucket_name):
    #Instantiate bucket
    bucket = gcs_client.get_bucket(bucket_name)

    #Download encrypted file to temp file
    encrypted_file = download_file(filename,bucket)

    #Decrypt file
    decrypted_file = run_decryption(encrypted_file,filename)

    #Upload decrypted file back to bucket
    upload_file(filename,decrypted_file,bucket)
