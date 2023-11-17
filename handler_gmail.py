from datetime import datetime
import imaplib
import email
from email.header import decode_header
from datetime import datetime
from email.utils import parsedate_to_datetime
import html2text
import pandas as pd
from evadb.third_party.types import DBHandler, DBHandlerResponse, DBHandlerStatus


class GmailHandler(DBHandler):
    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.email = kwargs.get("email") # email account
        self.password = kwargs.get("password") # Special password
        # To connect to gmail account
        self.imap_server = "imap.gmail.com"
        self.imap_port = 993

    def connect(self):
        # Tries the connection
        try:
            # First prepares the email server
            self.mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            # Tries to connect to the gmail account
            self.mail.login(self.email, self.password)
            # Returns if it has been able to connect
            return DBHandlerStatus(status=True)
        except Exception as e:
            # If it has not been able to connect return False
            return DBHandlerStatus(status=False, error=str(e))

    def disconnect(self):
        #Tries to disconnect
        try:
            # It disconnect
            self.mail.logout()
        except Exception as e:
            pass  # Handle logout error if needed

    def check_connection(self) -> DBHandlerStatus:
        try:
            # Checks if is receiving a signal from the mail account
            self.mail.noop()
        except Exception as e:
            return DBHandlerStatus(status=False, error=str(e))
        return DBHandlerStatus(status=True)
    
    def get_tables(self) -> DBHandlerResponse:
        # Each folder of the gmail account is a table
        try:
            status, folder_data = self.mail.list()
            folder_list = []
            if status == "OK":
                for folder_info in folder_data:
                    folder_info_str = folder_info.decode('utf-8')
                    _, folder_name = folder_info_str.split(' "/" ')
                    folder_name = folder_name.strip('"')
                    folder_list.append(folder_name)

            return DBHandlerResponse(data=folder_list)
        except Exception as e:
            return DBHandlerResponse(data=None, error=str(e))

    def get_columns(self, table_name: str) -> DBHandlerResponse:
        columns = [
            "sender",
            "receiver",
            "day",
            "subject",
            "message",
        ]
        columns_df = pd.DataFrame(columns, columns=["column_name"])
        return DBHandlerResponse(data=columns_df)

    def _decode_header(self, header):
        # Function to decode the program
        decoded, encoding = decode_header(header)[0]
        if isinstance(decoded, bytes):
            decoded = decoded.decode(encoding or "utf-8")
        return decoded

    def select(self, mailbox) -> DBHandlerResponse:
        try:
            self.mail.select(mailbox)
            status, messages = self.mail.search(None, "ALL")
            if status == "OK":
                for num in messages[0].split():
                    _, msg_data = self.mail.fetch(num, "(RFC822)")
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)

                    sender = self._decode_header(msg["From"])
                    receiver = self._decode_header(msg["To"])
                    subject = self._decode_header(msg["Subject"])
                    date_str = self._decode_header(msg["Date"])
                    date_object = parsedate_to_datetime(date_str)
                    # Convert date string to datetime object
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))

                            if "attachment" not in content_disposition:
                                payload = part.get_payload(decode=True)
                                if payload is not None:
                                    # If it's HTML, convert to plain text
                                    if content_type == "text/html":
                                        body += html2text.html2text(payload.decode("utf-8", "ignore"))
                                    else:
                                        body += payload.decode("utf-8", "ignore")
                    # If it is not multipart it gets easily the body
                    else:
                        payload = msg.get_payload(decode=True)
                        if payload is not None:
                            body = payload.decode("utf-8", "ignore")
                    yield DBHandlerResponse(data={
                        "sender": sender,
                        "receiver": receiver,
                        "day": date_object.strftime("%Y-%m-%d"),
                        "subject": subject,
                        "message": msg.get_payload(),
                    })

        except Exception as e:
            return DBHandlerResponse(data=None, error=str(e))
