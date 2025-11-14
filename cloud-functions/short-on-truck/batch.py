import os
import requests
import json
import functions_framework
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import workflows_v1

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

def get_cases_from_last_15_days():
    """Get case IDs from last 15 days"""
    try:
        token_response = get_oauth_token()
        access_token = token_response['access_token']
        
        # Calculate date 15 days ago
        fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Updated query with date filter for last 15 days
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Case/query"
        params = {
            'fields': 'Id, CaseNumber, Subject, Status, OwnerId',
            'filters': f"Subject LIKE '%Credit%' AND Status LIKE '%New%' AND OwnerId='00G0y000003TEGc' AND CreatedDate >= {fifteen_days_ago}"
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            case_ids = [record['Id'] for record in data.get('records', [])]
            return case_ids
        else:
            raise Exception(f"Failed to get cases: {response.text}")
            
    except Exception as e:
        raise Exception(f"Error getting cases: {str(e)}")

def trigger_workflow_for_case(case_id):
    """Trigger workflow for a single case ID"""
    try:
        from google.cloud.workflows import executions_v1
        
        client = executions_v1.ExecutionsClient()
        
        # Replace with your actual project, location, and workflow name
        parent = f"projects/{os.getenv('PROJECT_ID')}/locations/{os.getenv('LOCATION')}/workflows/{os.getenv('WORKFLOW_NAME')}"
        
        execution = executions_v1.Execution(
            argument=json.dumps({"caseid": case_id})
        )
        
        operation = client.create_execution(
            parent=parent,
            execution=execution
        )
        
        return {"success": True, "case_id": case_id, "execution_name": operation.name}
        
    except Exception as e:
        return {"success": False, "case_id": case_id, "error": str(e)}

@functions_framework.http
def batch_process_cases(request):
    """HTTP Cloud Function to get cases from last 15 days and trigger workflow for each"""
    try:
        # Get case IDs from last 15 days
        case_ids = get_cases_from_last_15_days()
        
        if not case_ids:
            return {"message": "No cases found from last 15 days", "case_count": 0}
        
        # Trigger workflow for each case
        results = []
        for case_id in case_ids:
            result = trigger_workflow_for_case(case_id)
            results.append(result)
        
        successful_triggers = [r for r in results if r["success"]]
        failed_triggers = [r for r in results if not r["success"]]
        
        return {
            "message": f"Processed {len(case_ids)} cases from last 15 days",
            "total_cases": len(case_ids),
            "successful_triggers": len(successful_triggers),
            "failed_triggers": len(failed_triggers),
            "case_ids": case_ids,
            "results": results
        }
        
    except Exception as e:
        return {"error": str(e)}, 500