# This code takes input from S3 put object
# Creates Transcription of audio file
# Generates pre-signed URL
# Send to slack-to-lambda solution provided by AWS
# Send to SES email

import json
import boto3
import os
import time
import urllib3


def lambda_handler(event, context):
    
    object_name = event['Records'][0]['s3']['object']['key']
    bucket_name = 'vm-enlapa-voicemailstack-ges-audiorecordingsbucket-vk1l1mbtof9p'
    expiration = 604800
    
    # Generate pre-signed URL for the object
    s3_client = boto3.client('s3')
    response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    print("VM pre-signed URL: " + response)
    s3_vm_link = response
    
    # Fetch information related to contact ID
    contact_id = object_name.split('_')[0][11:]
    
    ac_client = boto3.client('connect')
    response = ac_client.get_contact_attributes(
        InstanceId = 'a9795818-846d-4354-b0b3-b795f55c35ed',
        InitialContactId = contact_id
    )
    print('Get contact attribute response: ')
    print(response)
    customer_phone_number = response['Attributes']['customer_phone_number']
    caller_identity = response['Attributes']['call_purpose']
    language = response['Attributes']['language']
    #customer_phone_number = '+3232322432'
    #language = 'English'
    
    # Create Transcription of the audio
    job_name = contact_id
    transcribe = boto3.client('transcribe')
    object_url = 'https://s3.amazonaws.com/'+bucket_name+'/'+object_name
    language_code = 'es-US'
    if language == 'German':
        language_code = 'de-DE'
    
    response = transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        LanguageCode=language_code,
        MediaFormat='wav',
        Media={
            'MediaFileUri': object_url
        }
    )
    print("Transcription started")
    
    # Check transcription status
    while(True):
        response = transcribe.list_transcription_jobs(JobNameContains=job_name)
        if response['TranscriptionJobSummaries'][0]['TranscriptionJobStatus'] == 'COMPLETED':
            transcription_state = 'Complete'
            break
        if response['TranscriptionJobSummaries'][0]['TranscriptionJobStatus'] == 'FAILED':
            transcription_state = 'Failed'
            break
        time.sleep(1)
    print('Transcription status: ' + transcription_state)
    
    # Fetch transcription
    job = transcribe.get_transcription_job(TranscriptionJobName=job_name)
    uri = job['TranscriptionJob']['Transcript']['TranscriptFileUri']
    print("Transcription URI: " + uri)
    
    http = urllib3.PoolManager()
    content = http.request('GET', uri)
    data = json.loads(content.data.decode('utf-8'))
    transcript = data['results']['transcripts'][0]['transcript']
    
    # Invoking Lambda to send message to slack
    payload = [
        "Customer Phone number:" + customer_phone_number,
        "Customer category: " + caller_identity,
        "Transcript:" + transcript,
        "<"+s3_vm_link+"|Voicemail link>"
        ]
    print(payload)
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName='serverlessrepo-lambda-to-slack-LambdaToSlack-jsDFUzZlJL9A',
        Payload= bytes(json.dumps(payload), "utf-8"),
        InvocationType = "Event",
        Qualifier='1',
    )
    print(response)
    
    # Email parameters
    SOURCE_EMAIL = 'roeller@enlapa.de'
    DESTINATION_EMAILS = ['roeller@enlapa.de']
    
    subject = 'New voicemail received from ' + customer_phone_number + ' : ' + caller_identity
    message = '<html><body>Hello,<br><br>You have received a new voicemail from ' + customer_phone_number + '<br><br>Customer category: ' + caller_identity + '<br><br>Transcript: ' + transcript + '<br><br><a href="'+ s3_vm_link +'">Link to voicemail</a></body></html>'
    print(message)
    
    client = boto3.client('sesv2')
    response = client.send_email(
        FromEmailAddress='roeller@enlapa.de',
        FromEmailAddressIdentityArn='arn:aws:ses:eu-central-1:862239972127:identity/roeller@enlapa.de',
        Destination={
        'ToAddresses': [
            'roeller@enlapa.de'
        ]
        },
        Content={
        'Simple': {
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Html': {
                    'Data': message,
                    'Charset': 'UTF-8'
                }
            }
        }
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
