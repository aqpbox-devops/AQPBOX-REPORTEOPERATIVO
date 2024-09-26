from exchangelib import Credentials, Account, DELEGATE, ItemAttachment
from datetime import datetime
import io

def extract_xlsx_attachments(email, password, folder_name, date_match: datetime = None, last_n_months: int = 0):
    credentials = Credentials(email, password)
    account = Account(email, credentials=credentials, autodiscover=True, access_type=DELEGATE)

    folder = account.inbox / folder_name

    xlsx_buffers = []

    if date_match:
        matching_date = date_match.date()
        for item in folder.filter(datetime_received__year=matching_date.year,
                                  datetime_received__month=matching_date.month,
                                  datetime_received__day=matching_date.day).order_by('-datetime_received'):
            for attachment in item.attachments:
                if isinstance(attachment, ItemAttachment) and attachment.name.endswith('.xlsx'):
                    buffer = io.BytesIO()
                    buffer.write(attachment.content)
                    buffer.seek(0)
                    xlsx_buffers.append((attachment.name, buffer))
    else:
        unique_dates = set()
        current_month = datetime.now().month
        current_year = datetime.now().year
        found_months = 0
        
        for item in folder.all().order_by('-datetime_received'):
            received_date = item.datetime_received.date()
            
            if found_months >= last_n_months:
                break

            if (received_date.month, received_date.year) not in unique_dates:
                last_day_of_month = (received_date.replace(day=1).replace(month=received_date.month % 12 + 1) - timedelta(days=1)).day
                if received_date.day == last_day_of_month:
                    unique_dates.add((received_date.month, received_date.year))
                    found_months += 1

                    for attachment in item.attachments:
                        if isinstance(attachment, ItemAttachment) and attachment.name.endswith('.xlsx'):
                            buffer = io.BytesIO()
                            buffer.write(attachment.content)
                            buffer.seek(0)
                            xlsx_buffers.append((attachment.name, buffer))

    return xlsx_buffers

if __name__ == "__main__":
    email = "tu_email@ejemplo.com"
    password = "tu_contraseña"
    folder_name = "Bandeja de entrada"

    xlsx_files = extract_xlsx_attachments(email, password, folder_name)
    
    for file_name, buffer in xlsx_files:
        print(f"Archivo extraído: {file_name}, tamaño: {buffer.getbuffer().nbytes} bytes")
