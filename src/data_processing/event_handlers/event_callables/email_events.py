import os
from typing import Dict, Union, List
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

class Email:

    def __init__(self):
        region = "eu-west-1"
        self.client = boto3.Session().client('ses', region_name=region)
    def send(self, **kwargs: Dict[str, str] ):

        try:
            #Provide the contents of the email.
            response = self.client.send_email(
                Destination={
                    'ToAddresses': [
                        kwargs['recipient'],
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': kwargs['charset'],
                            'Data': kwargs['body_html'],
                        },
                        'Text': {
                            'Charset': kwargs['charset'],
                            'Data': kwargs['body_text'],
                        },
                    },
                    'Subject': {
                        'Charset': kwargs['charset'],
                        'Data': kwargs['subject'],
                    },
                },
                Source=kwargs['sender'],
                # If you are not using a configuration set, comment or delete the
                # following line
                #ConfigurationSetName=CONFIGURATION_SET,
            )
        # Display an error if something goes wrong.	
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

    def send_multipart(self, **kwargs: Dict[str, str]):
        multipart_content_subtype = 'alternative' if kwargs['body_text'] and kwargs['body_html'] else 'mixed'
        msg = MIMEMultipart(multipart_content_subtype)
        msg['Subject'] = kwargs['subject']
        msg['From'] = kwargs['sender']
        msg['To'] = ', '.join(kwargs['recipients'])

        # Record the MIME types of both parts - text/plain and text/html.
        # According to RFC 2046, the last part of a multipart message, in this case the HTML message, is best and preferred.
        if kwargs['body_text']:
            part = MIMEText(kwargs['body_text'], 'plain')
            msg.attach(part)
        if kwargs['body_html']:
            part = MIMEText(kwargs['body_html'], 'html')
            msg.attach(part)

        # Add attachments
        for attachment in kwargs['file'] or []:
            with open(attachment, 'rb') as f:
                part = MIMEApplication(f.read())
                part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
                msg.attach(part)
        try:
            response = self.client.send_raw_email(
            Source=kwargs['sender'],
            Destinations=kwargs['recipients'],
            RawMessage={'Data': msg.as_string()})
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])




def suspicious_data(event: str, params: Dict[str, str]) -> None:
    email = Email()
    ''' send an email when stuff is broken '''
    print("sent an email")
    sender = "xxx"
    body_html = """
            <html>
            <head></head>
            <body>
                <h1>Amazon SES Test (SDK for Python)</h1>
                <p>This email was sent with
                    <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
                    <a href='https://aws.amazon.com/sdk-for-python/'>
                    AWS SDK for Python (Boto)</a>.</p>
            </body>
            </html>
        """   
    charset = "UTF-8"
    email.send_multipart(sender=sender, recipients=params['recipients'], subject=params['subject'], body_text=params['body'], body_html=body_html, charset=charset, file=[params['file']])
    