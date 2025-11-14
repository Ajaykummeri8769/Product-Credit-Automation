
import os
import requests
import json
import functions_framework
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

@functions_framework.http
def send_to_validation(request):
    """HTTP Cloud Function with advanced validation and data resolution"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "No case details provided"}, 400
        
        agent_response_data = request_json.get('agent_response_data')
        case_details = request_json.get('case_details')
        
        if not agent_response_data:
            return {"error": "No agent response data provided"}, 400
        
        # Advanced validation with data resolution
        validation_results = validate_agent_response(agent_response_data, case_details)
        
        if not validation_results.get('overall_valid', False):
            return {"Invoice_results": "Validation failed as given accountId/opcode is invalid"}
        
        # Process CES validation
        sf_Details = validation_results['validated_data']
        ces_results = ces_process_credit_eligibility(sf_Details)
        return {"Invoice_results": ces_results}
        
    except Exception as e:
        return {"error": str(e)}, 500

def get_oauth_token():
    """Get OAuth token for Salesforce APIs"""
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

def get_ces_oauth_token():
    """Get OAuth token for CES APIs (prod)"""
    token_url = f"{os.getenv('CES_GATEWAY_URL', os.getenv('GATEWAY_URL'))}/token"
   
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('CES_CLIENT_ID', os.getenv('CLIENT_ID')),
        'client_secret': os.getenv('CES_CLIENT_SECRET', os.getenv('CLIENT_SECRET'))
    }
   
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get CES token: {response.text}")

def ces_get_first_invoice_details(invoice_num, OpCo):
    """Validate invoice number"""
    try:
        token_response = get_ces_oauth_token()
        headers = {'Authorization': f'Bearer {token_response["access_token"]}', 'accept': 'application/json'}
        url = f"{os.getenv('CES_GATEWAY_URL', os.getenv('GATEWAY_URL'))}/services/enterprise-invoice-service-v2/invoice/details/opcos/{OpCo}/invoices/{invoice_num}"
        params = {"page_size" : 10000}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data and data.get('totalItems') > 0:
                return data
            else:
                return {"items": []}
        else:
            return {"items": []}
    except Exception as e:
        return {"items": [], "error": str(e)}
 
def validate_account(account_id, headers):
    """Validate account ID"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Account/query"
        params = {'fields': 'Account_ID__c', 'filters': f"Account_ID__c='{account_id}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalSize', 0) > 0:
                return data['records'][0]['Account_ID__c'] == account_id
        return False
    except:
        return False

def validate_opco(opco_id, headers):
    """Validate OpCo code"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/OpCo__c/query"
        params = {'fields': 'OpCo_ID__c', 'filters': f"OpCo_ID__c = '{opco_id}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalSize', 0) > 0:
                return data['records'][0]['OpCo_ID__c'] == opco_id
        return False
    except:
        return False

def parse_account_id(account_id):
    """Parse account ID to extract OpCo(3) and AccountNumber(5-6)"""
    if not account_id or account_id == "I'm not sure":
        return None, None
    
    account_id = account_id.strip()
    
    # Format: OpCo-AccountNumber (ABC-12345)
    if '-' in account_id:
        parts = account_id.split('-', 1)
        opco = parts[0]
        account_num = parts[1]
        
        if len(opco) == 3 and len(account_num) in [5, 6] and account_num.isdigit():
            return opco, account_num
    
    # Format: OpCoAccountNumber (ABC12345)
    elif len(account_id) in [8, 9]:
        if account_id[:3].isalpha() and account_id[3:].isdigit():
            opco = account_id[:3]
            account_num = account_id[3:]
            if len(account_num) in [5, 6]:
                return opco, account_num
    
    # Format: AccountNumber only (12345)
    elif len(account_id) in [5, 6] and account_id.isdigit():
        return None, account_id
    
    return None, None

def build_account_id(opco, account_num):
    """Build proper account ID format"""
    if opco and account_num:
        return f"{opco}-{account_num}"
    return None

def get_account_from_invoice(invoice_num, headers):
    """Get account ID from invoice number"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Invoice__c/query"
        params = {'fields': 'Account__c', 'filters': f"Invoice_Number__c='{invoice_num}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalSize', 0) > 0:
                return data['records'][0]['Account__c']
        return None
    except:
        return None

def get_opco_from_account_number(account_num, headers, customer_name=None, invoice_num=None):
    """Get OpCo from account number with additional context"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Account/query"
        params = {'fields': 'OpCo__c, Name', 'filters': f"Account_Number__c='{account_num}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            if len(records) == 1:
                return records[0]['OpCo__c']
            elif len(records) > 1:
                return f"Multiple OpCos found: {[r['OpCo__c'] for r in records]}"
        return None
    except:
        return None



def get_supcs_from_invoice(invoice_num, headers):
    """Get available SUPCs from invoice"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Invoice_Line_Item__c/query"
        params = {'fields': 'SUPC__c', 'filters': f"Invoice__c.Invoice_Number__c='{invoice_num}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            return [record['SUPC__c'] for record in data.get('records', [])]
        return []
    except:
        return []

def get_customer_name_from_account(account_id, headers):
    """Get customer name from account ID"""
    try:
        url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Account/query"
        params = {'fields': 'Name', 'filters': f"Account_ID__c='{account_id}'"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalSize', 0) > 0:
                return data['records'][0]['Name']
        return None
    except:
        return None

def validate_agent_response(agent_response_data, case_details=None):
    """Advanced validation with data resolution"""
    try:
        agent_responses = agent_response_data.get('agent_response', [])
        if not agent_responses:
            return {'valid': False, 'error': 'No agent response data'}
        
        response_data = agent_responses[0]
        
        # Get OAuth token
        token_response = get_oauth_token()
        access_token = token_response['access_token']
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'accept': 'application/json'
        }
        
        validation_results = {
            'account_validation': True,
            'opco_validation': True,
            'overall_valid': True,
            'resolved_account_id': None,
            'resolved_opco': None,
            'validated_data': {},
            'headers': headers
        }
        
        # Enhanced Account/Invoice Logic
        account_id = response_data.get('CustomerNumber_AccountId')
        opco_id = response_data.get('OpCoCode')
        invoice_num = None
        
        # Parse and validate account ID format
        parsed_opco, parsed_account_num = parse_account_id(account_id)
        
        # Resolve OpCo and Account ID
        if parsed_opco and parsed_account_num:
            opco_id = parsed_opco
            account_id = build_account_id(parsed_opco, parsed_account_num)
            validation_results['resolved_account_id'] = account_id
            validation_results['resolved_opco'] = opco_id
        elif parsed_account_num and opco_id and opco_id != "I'm not sure":
            account_id = build_account_id(opco_id, parsed_account_num)
            validation_results['resolved_account_id'] = account_id
        
        # Get invoice from credit requests
        for credit_request in response_data.get('CreditRequests', []):
            if credit_request.get('InvoiceNumber') and credit_request.get('InvoiceNumber') != "I'm not sure":
                invoice_num = credit_request.get('InvoiceNumber')
                break
        
        # Get account ID if not provided
        if not account_id or account_id == "I'm not sure":
            if invoice_num:
                account_id = get_account_from_invoice(invoice_num, headers)
                validation_results['resolved_account_id'] = account_id
                if account_id:
                    resolved_opco, _ = parse_account_id(account_id)
                    if resolved_opco:
                        opco_id = resolved_opco
                        validation_results['resolved_opco'] = opco_id
        
        # If OpCo is missing but we have account number, try to find OpCo
        if (not opco_id or opco_id == "I'm not sure") and account_id and len(account_id) in [5, 6] and account_id.isdigit():
            customer_name = response_data.get('CustomerName')
            opco_result = get_opco_from_account_number(account_id, headers, customer_name, invoice_num)
            if opco_result and not opco_result.startswith("Multiple"):
                opco_id = opco_result
                validation_results['resolved_opco'] = opco_id
                account_id = build_account_id(opco_id, account_id)
                validation_results['resolved_account_id'] = account_id
            elif opco_result and opco_result.startswith("Multiple"):
                validation_results['opco_validation'] = False
                validation_results['multiple_opcos'] = opco_result
        
        # Validate resolved data
        if account_id:
            validation_results['account_validation'] = validate_account(account_id, headers)
        
        # Resolve missing SUPCs from invoice
        credit_requests = response_data.get('CreditRequests', [])
        for i, credit_request in enumerate(credit_requests):
            supc = credit_request.get('SUPC')
            if not supc or supc == "I'm not sure":
                if invoice_num:
                    available_supcs = get_supcs_from_invoice(invoice_num, headers)
                    if available_supcs:
                        credit_requests[i]['available_supcs'] = available_supcs
                        if len(available_supcs) == 1:
                            credit_requests[i]['SUPC'] = available_supcs[0]
        
        if opco_id and opco_id != "I'm not sure":
            validation_results['opco_validation'] = validate_opco(opco_id, headers)
        else:
            validation_results['opco_validation'] = False
        
        # Overall validation
        validation_results['overall_valid'] = (
            validation_results['account_validation'] and 
            validation_results['opco_validation']
        )
        
        # Resolve customer name if missing
        customer_name = response_data.get('CustomerName')
        if (not customer_name or customer_name == "I'm not sure") and account_id:
            customer_name = get_customer_name_from_account(account_id, headers)
        
        # Prepare validated data
        validation_results['validated_data'] = {
            'account_id': account_id,
            'invoice_number': invoice_num,
            'opco_code': opco_id,
            'delivery_date': response_data.get('DeliveryDate'),
            'customer_name': customer_name,
            'case_description': response_data.get('CaseDescription'),
            'credit_requests': credit_requests,
            'CaseCreationDate': case_details.get('created_date') if case_details else date.today().strftime('%Y-%m-%d')
        }
        
        return validation_results
        
    except Exception as e:
        return {'valid': False, 'error': str(e)}

def ces_get_scanned_invoice(invoice_number, opco_number):
    """Get scanned invoice from CES API"""
    try:
        token_response = get_ces_oauth_token()
        headers = {'Authorization': f'Bearer {token_response["access_token"]}', 'accept': 'application/json'}
        url = f"{os.getenv('CES_GATEWAY_URL', os.getenv('GATEWAY_URL'))}/services/enterprise-invoice-service-v2/invoice/details/opcos/{opco_number}/invoices/{invoice_number}/delivery"
        params = {"page_size" : 10000}
        response = requests.get(url, headers=headers,params=params)
        if response.status_code == 200:
            data = response.json()
            if data and data.get('totalItems', 0) > 0:
                return data
            else:
                return {"items": []}
        else:
            return {"items": []}
    except Exception as e:
        return {"items": [], "error": str(e)}
def ces_get_invoice_details(customer_number, OpCo, scheduledDeliveryDate, todayDate):
    """Get invoice details from CES API"""
    try:
        token_response = get_ces_oauth_token()
        headers = {'Authorization': f'Bearer {token_response["access_token"]}', 'accept': 'application/json'}
        url = f"{os.getenv('CES_GATEWAY_URL', os.getenv('GATEWAY_URL'))}/services/enterprise-invoice-service-v2/invoice/extended/details/opcos/{OpCo}/customers/{customer_number}"
        params = {"date_from":scheduledDeliveryDate, "date_to":todayDate, "page_size" : 10000}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data and data.get('totalItems', 0) > 0:
                return data
           
            else:
                return {"items": []}
        else:
            return {"items": []}
    except Exception as e:
        return {"items": [], "error": str(e)}
 

def ces_process_credit_eligibility(sf_Details):
    """Process credit eligibility based on business logic"""
    results = []
   
    try:
        if isinstance(sf_Details, dict):
            sf_Details = [sf_Details]
        elif not isinstance(sf_Details, list):
            return {'invoice_number': '', 'data': []}
    except Exception as e:
        return {'invoice_number': '', 'data': []}
 
    for code_data in sf_Details:
        try:
            if not isinstance(code_data, dict):
                continue
           
            opco_number = code_data.get('opco_code')
            if not opco_number:
                raise ValueError(f"OpCoCode is missing")
 
            customer_account_id = code_data.get('account_id')
            if not customer_account_id or '-' not in customer_account_id:
                raise ValueError(f"account_Id is missing or invalid format")
 
            customer_number = customer_account_id.split('-')[1]
            if not customer_number:
                raise ValueError(f"Customer number could not be extracted")
 
            caseCreationDate = code_data.get('CaseCreationDate')
            if not caseCreationDate:
                caseCreationDate = datetime.now()
 
            # Parse case creation date
            try:
                if isinstance(caseCreationDate, str):
                    try:
                        caseCreationDate = datetime.strptime(caseCreationDate, "%Y-%m-%dT%H:%M:%S.%f%z")
                        # Convert to naive datetime
                        caseCreationDate = caseCreationDate.replace(tzinfo=None)
                    except ValueError:
                        try:
                            caseCreationDate = datetime.strptime(caseCreationDate, "%Y-%m-%d")
                        except ValueError as e:
                            raise ValueError(f"Invalid CaseCreationDate format: {e}")
                elif isinstance(caseCreationDate, date):
                    caseCreationDate = datetime.combine(caseCreationDate, datetime.min.time())
            except Exception as e:
                raise Exception(f"Error parsing CaseCreationDate: {e}")
 
            credit_requests = code_data.get('credit_requests', [])
            if not credit_requests:
                raise ValueError(f"credit_requests is missing or empty")
 
            for j, credit_req in enumerate(credit_requests):
                try:
                    if not isinstance(credit_req, dict):
                        raise ValueError(f"credit_requests[{j}] must be a dictionary")
 
                    invoice_number = credit_req.get('InvoiceNumber')
                    if not invoice_number:
                        raise ValueError(f"InvoiceNumber is missing in credit_requests[{j}]")
 
                    supc = credit_req.get('SUPC')
                    if not supc:
                        raise ValueError(f"SUPC is missing in credit_requests[{j}]")
 
                    qty = credit_req.get('MissingQuantity')
                    try:
                        requested_qty = abs(int(qty)) if qty else 0
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid QTY format in credit_requests[{j}]: {qty}")
 
                    try:
                        original_invoice_data = ces_get_first_invoice_details(invoice_number, opco_number)
                    except Exception as e:
                        raise Exception(f"Failed to get scanned invoice data for invoice {invoice_number}: {e}")
                   
                    if not original_invoice_data or not isinstance(original_invoice_data, dict) or not original_invoice_data.get('items'):
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'status': 'invoice data not found',
                            'eligible': False
                        })
                        continue
 
                    # Find matching item in scanned data by SUPC
                    original_invoice_item = None
                    for item in original_invoice_data.get('items', []):
                        if item.get('itemNumber') == supc:
                            original_invoice_item = item
                            break
 
                    if not original_invoice_item:
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'status': 'Item not found in main invoice',
                            'eligible': False
                        })
                        continue
 
                    try:
                        splitCode = original_invoice_item.get('splitCode')
                        if splitCode=="S":
                            splitCode="S"
                        else:
                            splitCode="CS"
                    except Exception as e:
                        raise Exception(f"Error validating scanned item data for SUPC {supc}: {e}")
 
                    # Get scanned invoice data
                    try:
                        scanned_data = ces_get_scanned_invoice(invoice_number, opco_number)
                    except Exception as e:
                        raise Exception(f"Failed to get scanned invoice data for invoice {invoice_number}: {e}")
 
                    # Handle scanned data not found
                    if not scanned_data or not isinstance(scanned_data, dict) or not scanned_data.get('items'):
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'status': 'Scanned data not found',
                            'eligible': False
                        })
                        continue
 
                    # Find matching item in scanned data by SUPC
                    scanned_item = None
                    for item in scanned_data.get('items', []):
                        if item.get('itemNumber') == supc:
                            scanned_item = item
                            break
 
                    if not scanned_item:
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'status': 'Item not found in scanned invoice',
                            'eligible': False
                        })
                        continue
 
                    # Validate scanned item data
                    try:
                        quantity = scanned_item.get('quantity')
                        if quantity is None:
                            raise ValueError(f"quantity is missing in scanned item for SUPC {supc}")
                        quantity = int(quantity)
 
                        delivered_qty = scanned_item.get('deliveredItemQty', 0)
                        delivered_qty = int(delivered_qty) if delivered_qty is not None else 0
 
                        rejected_qty = scanned_item.get('rejectedItemQty', 0)
                        rejected_qty = int(rejected_qty) if rejected_qty is not None else 0
 
                        scheduledDeliveryDate = scanned_item.get('scheduledDeliveryDate')
                        if not scheduledDeliveryDate:
                            raise ValueError(f"scheduledDeliveryDate is missing in scanned item for SUPC {supc}")
 
                        # Parse delivery date
                        try:
                            eligibleDate = datetime.strptime(scheduledDeliveryDate, '%Y-%m-%d')
                        except ValueError as e:
                            raise ValueError(f"Invalid scheduledDeliveryDate format for SUPC {supc}: {e}")
 
                    except Exception as e:
                        raise Exception(f"Error validating scanned item data for SUPC {supc}: {e}")
 
                    # Calculate time differences
                    todayDate = date.today()
                    duration = caseCreationDate - eligibleDate
                    hours = duration.total_seconds() / 3600
                    days = hours/24
                    
                    # Business logic checks
                    if hours < 24:
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'splitCode': splitCode,
                            'status': 'On Hold - Case created within 24 hours of delivery',
                            'eligible': False,
                            'quantity': quantity,
                            'delivered_rejected_sum': delivered_qty + rejected_qty,
                            'scanned_item': scanned_item,
                            'invoice_item': None
                        })
                        continue
 
                    if days>14:
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'splitCode': splitCode,
                            'status': 'On Hold - Case created after 14 days of delivery',
                            'eligible': False,
                            'quantity': quantity,
                            'delivered_rejected_sum': delivered_qty + rejected_qty,
                            'scanned_item': scanned_item,
                            'invoice_item': None
                        })
                        continue
 
                    elif quantity > (delivered_qty + rejected_qty):
                        # Get invoice details for customer
                        try:
                            invoice_details = ces_get_invoice_details(customer_number, opco_number, scheduledDeliveryDate, todayDate)
                        except Exception as e:
                            raise Exception(f"Failed to get invoice details for customer {customer_number}: {e}")
 
                        # Check for refInvoice matching with status=C
                        ref_invoice_found = False
                        matching_item = None
                        original_ship_qty = 0
 
                        for detail_item in invoice_details.get('items', []):
                            if (detail_item.get('invoiceRefNumber') == invoice_number and
                                detail_item.get('itemNumber') == supc and
                                detail_item.get('transCode') == 'C'):
                               
                                ref_invoice_found = True
                                matching_item = detail_item
                                ship_qty = detail_item.get('originalShipQty', 0)
                                try:
                                    original_ship_qty += int(ship_qty) if ship_qty is not None else 0
                                except (ValueError, TypeError):
                                    raise ValueError(f"Invalid originalShipQty format: {ship_qty}")
 
                        if ref_invoice_found and matching_item:
                            # Compare quantities
                            scanned_difference = (delivered_qty + rejected_qty) - quantity
                           
                            if scanned_difference == original_ship_qty:
                                results.append({
                                    'invoice_number': invoice_number,
                                    'supc': supc,
                                    'splitCode': splitCode,
                                    'status': 'Credits not eligible as exact quantities match with previous processed credit',
                                    'eligible': False,
                                    'scanned_difference': scanned_difference,
                                    'original_ship_qty': original_ship_qty,
                                    'scanned_item': scanned_item,
                                    'invoice_item': matching_item
                                })
                            elif scanned_difference > original_ship_qty:
                                results.append({
                                    'invoice_number': invoice_number,
                                    'supc': supc,
                                    'splitCode': splitCode,
                                    'status': 'Lesser credits eligible as partial credits are already processed',
                                    'eligible': True,
                                    'scanned_difference': scanned_difference,
                                    'original_ship_qty': original_ship_qty,
                                    'scanned_item': scanned_item,
                                    'invoice_item': matching_item
                                })
                            else:
                                results.append({
                                    'invoice_number': invoice_number,
                                    'supc': supc,
                                    'splitCode': splitCode,
                                    'status': 'Eligible for credit',
                                    'eligible': True,
                                    'scanned_difference': scanned_difference,
                                    'original_ship_qty': original_ship_qty,
                                    'scanned_item': scanned_item,
                                    'invoice_item': matching_item
                                })
                        else:
                            # No previous credits found - eligible
                            results.append({
                                'invoice_number': invoice_number,
                                'supc': supc,
                                'splitCode': splitCode,
                                'status': 'Eligible for credit as no previous processed credits has found',
                                'eligible': True,
                                'quantity': quantity,
                                'delivered_rejected_sum': delivered_qty + rejected_qty,
                                'scanned_item': scanned_item,
                                'invoice_item': None
                            })
                    else:
                        # Fully delivered/rejected
                        results.append({
                            'invoice_number': invoice_number,
                            'supc': supc,
                            'splitCode': splitCode,
                            'status': 'Not eligible - the order is fully loaded on truck. Customer get delivered/rejected either partial/full order qty',
                            'eligible': False,
                            'quantity': quantity,
                            'delivered_rejected_sum': delivered_qty + rejected_qty,
                            'scanned_item': scanned_item,
                            'invoice_item': None
                        })
 
                except Exception as e:
                    raise Exception(f"Error processing credit request {j}: {e}")
 
        except Exception as e:
            raise Exception(f"Error processing sf_Details: {e}")
 
        # Group results by invoice
        try:
            grouped_results = {}
            for result in results:
                invoice_key = result['invoice_number']
                if invoice_key not in grouped_results:
                    grouped_results[invoice_key] = {
                        "invoice": invoice_key,
                        "credits_eligibility": []
                    }
 
                # Calculate sot_credits_eligible
                scanned_item = result.get('scanned_item', {})
                quantity = scanned_item.get('quantity', 0)
                splitCode= result.get('splitCode')
                delivered_qty = scanned_item.get('deliveredItemQty', 0)
                rejected_qty = scanned_item.get('rejectedItemQty', 0)
                original_ship_qty = result.get('invoice_item', {}).get('originalShipQty', 0) if result.get('invoice_item') else 0
                sot_credits_eligible = quantity - delivered_qty - rejected_qty + original_ship_qty
 
                # Find original requested quantity from sf_Details
                requested_qty = 0
                for item in sf_Details:
                    for credit_req in item.get('credit_requests', []):
                        if (credit_req.get('SUPC') == result['supc'] and
                            credit_req.get('InvoiceNumber') == invoice_key):
                            try:
                                requested_qty = int(credit_req.get('MissingQuantity', 0))
                            except (ValueError, TypeError):
                                requested_qty = 0
                            break
 
                credit_item = {
                    "SUPC": result['supc'],
                    "splitCode": splitCode,
                    "sot_credits_requested": requested_qty,
                    "sot_credits_eligible": sot_credits_eligible,
                    "Status": result['status'],
                    "eligibility": result['eligible'],
                    "OrderedQuantity": quantity,
                    "DeliveredQuantity": delivered_qty,
                    "rejectedQuantity": rejected_qty,
                    "previousCreditsAvailedQty": -original_ship_qty,
                    "deliveryDate": scanned_item.get('scheduledDeliveryDate', '')
                }
                grouped_results[invoice_key]["credits_eligibility"].append(credit_item)
           
            return list(grouped_results.values())
           
        except Exception as e:
            raise Exception(f"Error grouping results: {e}")