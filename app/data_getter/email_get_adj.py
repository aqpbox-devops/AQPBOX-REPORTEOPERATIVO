from exchangelib import Credentials, Account, DELEGATE, ItemAttachment
import io

def extract_xlsx_attachments(email, password, folder_name):
    
    credentials = Credentials(email, password)
    account = Account(email, credentials=credentials, autodiscover=True, access_type=DELEGATE)

    folder = account.inbox / folder_name

    xlsx_buffers = []

    for item in folder.all().order_by('-datetime_received')[:10]:
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
