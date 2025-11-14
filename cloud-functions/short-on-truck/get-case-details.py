import os
import requests
import functions_framework
from dotenv import load_dotenv
load_dotenv()

def get_oauth_token():
    """Get OAuth token using client credentials via API gateway"""
    token_url = f"{os.getenv('GATEWAY_URL')}/token"
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET')
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get token: {response.text}")

def update_case_status(case_id, access_token):
    """Update case status to In Progress"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    update_url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Case/{case_id}"
    update_data = {"Status": "In Progress"}
    
    response = requests.patch(update_url, headers=headers, json=update_data)
    if response.status_code not in [200, 204]:
        raise Exception(f"Failed to update case status: {response.text}")

@functions_framework.http
def get_case_details(request):
    """HTTP Cloud Function to get case details from Salesforce by case ID using OAuth"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "No case details provided"}, 400
        
        # Extract case ID
        case_id = request_json.get('case_id') or request_json.get('caseId')
            
        if not case_id:
            return {"error": "No case_id provided"}, 400
        
        # Get OAuth token
        token_response = get_oauth_token()
        access_token = token_response['access_token']
        
        # Update case status to In Progress
        update_case_status(case_id, access_token)
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Get case details via API gateway
        case_url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Case/{case_id}"
        case_response = requests.get(case_url, headers=headers)
        
        if case_response.status_code != 200:
            return {"error": f"Failed to get case: {case_response.text}"}
        
        case = case_response.json()
        
        # Get email messages via API gateway
        fields = "Id, Subject, FromAddress, FromName, ToAddress, TextBody, CreatedDate"
        email_url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/EmailMessage/query"
        params = {
            'fields': fields,
            'filters': f"ParentId='{case_id}'"
        }
        email_response = requests.get(email_url, headers=headers, params=params)
        
        emails = email_response.json() if email_response.status_code == 200 else {'totalSize': 0, 'records': []}
        
        # Format case data
        case_data = {
            'id': case['Id'],
            'case_number': case.get('CaseNumber', ''),
            'subject': case.get('Subject', ''),
            'description': case.get('Description', ''),
            'status': case.get('Status', ''),
            'priority': case.get('Priority', ''),
            'type': case.get('Type', ''),
            'origin': case.get('Origin', ''),
            'created_date': case.get('CreatedDate', ''),
            'Account_ID__c':case.get('Account_ID__c',''),
            'last_modified_date': case.get('LastModifiedDate', ''),
            'closed_date': case.get('ClosedDate', ''),
            'is_closed': case.get('IsClosed', False),
            'account_name': '',
            'contact_name': '',
            'contact_email': case.get('ContactEmail', ''),
            'owner_name': '',
            'owner_type': '',
            'email_messages': [],
            'email_count': emails.get('totalSize', 0)
        }
        
        # Add email messages
        for email in emails.get('records', []):
            email_data = {
                'id': email['Id'],
                'subject': email.get('Subject', ''),
                'from_address': email.get('FromAddress', ''),
                'from_name': email.get('FromName', ''),
                'to_address': email.get('ToAddress', ''),
                'text_body': email.get('TextBody', ''),
                'created_date': email.get('CreatedDate', '')
            }
            case_data['email_messages'].append(email_data)
        
        return case_data
        
    except Exception as e:
        return {"error": str(e)}, 500