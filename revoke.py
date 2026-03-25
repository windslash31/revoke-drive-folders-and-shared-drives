import os
import json
from collections import deque
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive']

ROOT_FOLDER_ID = 'xxxxx'

SAFE_EMAILS = [
    'x'
]

def authenticate():
    """Handles the OAuth2 login flow."""
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            # This triggers the browser login window
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return creds

def clean_item(service, item_id, item_name, safe_users):
    """Removes link sharing and unauthorized users from a specific file/folder."""
    try:
        results = service.permissions().list(
            fileId=item_id, 
            supportsAllDrives=True, 
            fields="permissions(id, emailAddress, type, role)"
        ).execute()
        
        permissions = results.get('permissions', [])
        
        for perm in permissions:
            perm_id = perm.get('id')
            perm_type = perm.get('type')
            email = perm.get('emailAddress', '').lower()
            role = perm.get('role')

            if role == 'owner':
                continue

            # A. Remove "Anyone with link" or "Domain with link"
            if perm_type in ['anyone', 'domain']:
                service.permissions().delete(
                    fileId=item_id, 
                    permissionId=perm_id, 
                    supportsAllDrives=True
                ).execute()
                print(f"   🔒 Link Sharing Removed: {item_name}")

            # B & C. Remove Editors and Viewers
            elif perm_type == 'user' or perm_type == 'group':
                if email not in safe_users:
                    try:
                        service.permissions().delete(
                            fileId=item_id, 
                            permissionId=perm_id, 
                            supportsAllDrives=True
                        ).execute()
                        print(f"   👋 Removed {role}: {email} from {item_name}")
                    except HttpError as error:
                        pass

    except HttpError as error:
        print(f"❌ Error processing {item_name}: {error}")

def main():
    print('--- STARTING CLEANUP ---')
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    about = service.about().get(fields="user").execute()
    my_email = about['user']['emailAddress'].lower()
    
    all_safe_emails = [email.lower() for email in SAFE_EMAILS]
    if my_email not in all_safe_emails:
        all_safe_emails.append(my_email)
        
    print(f"Authenticated as: {my_email}")

    queue = deque([(ROOT_FOLDER_ID, True, "Root Folder")])

    while queue:
        current_id, is_folder, name = queue.popleft()
        
        print(f'\n📂 Processing: {name}')
        clean_item(service, current_id, name, all_safe_emails)

        if is_folder:
            try:
                page_token = None
                while True:
                    query = f"'{current_id}' in parents and trashed = false"
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields='nextPageToken, files(id, name, mimeType)',
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        pageToken=page_token
                    ).execute()

                    for file in response.get('files', []):
                        is_child_folder = file.get('mimeType') == 'application/vnd.google-apps.folder'
                        queue.append((file.get('id'), is_child_folder, file.get('name')))

                    page_token = response.get('nextPageToken', None)
                    if page_token is None:
                        break
                        
            except HttpError as error:
                print(f'❌ Error fetching children of {name}: {error}')

    print('--- ALL DONE ---')

if __name__ == '__main__':
    main()