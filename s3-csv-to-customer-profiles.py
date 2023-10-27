# Trigger this Lambda function by S3 put operation when new CSV file is uploaded
# Parse the CSV file and put profile in Customer profiles

import boto3
import csv

def lambda_handler(event, context):
    
    s3_bucket_name = 'amazon-connect-323a8e6aac75'
    #s3_object_key = 'data/'+event['Records'][0]['s3']['object']['key']
    s3_object_key = 'data/'+'amazon-connect-sample-data-1.csv'
    
    # Fetch the CSV file data
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
    data = response['Body'].read().decode('utf-8').splitlines()
    records = csv.reader(data)
    
    skip_first_line = True
    
    # Parsing data from CSV
    for eachRecord in records:
        if skip_first_line:
            skip_first_line = False
            continue
        
        id = eachRecord[0]
        first_name = eachRecord[1]
        last_name = eachRecord[2]
        add = eachRecord[5]
        city = eachRecord[7]
        county = eachRecord[8]
        state = eachRecord[9]
        postal_code = eachRecord[10]
        landline = eachRecord[19]
        email = eachRecord[22]
        google_url = eachRecord[23]
        phone = eachRecord[37]
        print(id, first_name, last_name, add, city, county, state, postal_code, landline, email, google_url, phone)
        
        # Validate parameter
        validation_failed, id, first_name, last_name, add, city, county, state, postal_code, landline, email, google_url, phone =  validate_parameters(id, first_name, last_name, add, city, county, state, postal_code, landline, email, google_url, phone)
        if validation_failed:
            continue
        
        # Add data in Customer profiles
        domain_name = 'CustomerProfileDomain2'
        customer_profiles = boto3.client('customer-profiles')
        response = customer_profiles.create_profile(
            DomainName=domain_name,
            AccountNumber=id,
            FirstName=first_name,
            LastName=last_name,
            PhoneNumber=phone,
            HomePhoneNumber=landline,
            EmailAddress=email,
            Address={
                'Address1': add,
                'City': city,
                'County': county,
                'State': state,
                'PostalCode': postal_code
            },
            AdditionalInformation=google_url
        )
        print('Profile id: ', response['ProfileId'])
    
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
    
def validate_parameters(id, first_name, last_name, add, city, county, state, postal_code, landline, email, google_url, phone):
    validation_failed = False
    test = 'test'
    if id is None:
        print('There is no valid ID for the record')
        validation_failed = True
    if first_name is None:
        first_name = ' '
    if last_name is None:
        last_name = ' '
    if add is None:
        add = ' '
    if city is None:
        city = ' '
    if county is None:
        county = ' '
    if state is None:
        state = ' '
    if postal_code is None:
        postal_code = ' '
    if landline is None:
        landline = 'default'
    if email is None:
        email = 'default'
    if google_url is None:
        google_url = 'default'
    if phone is None:
        validation_failed = True
    if test is None:
        test ='default'
    return validation_failed, id, first_name, last_name, add, city, county, state, postal_code, landline, email, google_url, phone
