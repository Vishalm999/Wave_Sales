import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from semantic.orchestrator import SemanticOrchestrator
from semantic.watsonx_adapter import WatsonxSemanticAdapter

load_dotenv()


class WatsonxChatModel:
    """
    Wrapper for WatsonX Chat API with automatic token management.
    """
    
    def __init__(self, project_id: str, api_key: str, model_id: str = "openai/gpt-oss-120b"):
        self.url = "https://us-south.ml.cloud.ibm.com/ml/v1/text/chat?version=2023-05-29"
        self.project_id = project_id
        self.api_key = api_key
        self.model_id = model_id
        
        # Token cache
        self._access_token = None
        self._token_expiry = None
        
        # Model parameters
        self.params = {
            "frequency_penalty": 0,
            "max_tokens": 2000,
            "presence_penalty": 0,
            "temperature": 0.1,
            "top_p": 1
        }
    
    def _get_access_token(self) -> str:
        """
        Get or refresh WatsonX access token.
        Token is cached and auto-refreshed when expired.
        """
        # Return cached token if still valid
        if self._access_token and self._token_expiry:
            if datetime.now() < self._token_expiry:
                return self._access_token
        
        # Generate new token
        print("Generating new WatsonX access token...")
        
        token_url = "https://iam.cloud.ibm.com/identity/token"
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        
        data = {
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": self.api_key
        }
        
        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=10)
            
            if response.status_code == 200:
                token_data = response.json()
                self._access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
                
                # Set expiry 5 minutes before actual expiry (safety margin)
                self._token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
                
                print(f"Token generated (expires in {expires_in}s)")
                return self._access_token
            else:
                print(f"Token generation failed: {response.status_code}")
                print(f"Response: {response.text}")
                raise Exception(f"Failed to get access token: {response.text}")
                
        except Exception as e:
            print(f"Token generation exception: {str(e)}")
            raise
    
    def generate_text(self, prompt: str) -> str:
        """Generate text using WatsonX Chat API."""
        
        # Get valid access token
        access_token = self._get_access_token()
        
        body = {
            "project_id": self.project_id,
            "model_id": self.model_id,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a precise JSON extraction assistant. Always return valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            **self.params
        }
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        try:
            response = requests.post(self.url, headers=headers, json=body, timeout=30)
            
            if response.status_code != 200:
                print(f"[ERROR] WatsonX API Error: {response.status_code}")
                print(f"Response: {response.text}")
                
                # Return fallback JSON
                return '{"metric": "total_sales", "dimensions": [], "date_range": "current_financial_year"}'
            
            data = response.json()
            
            # Extract response from chat format
            if "choices" in data and len(data["choices"]) > 0:
                return data["choices"][0]["message"]["content"]
            else:
                print(f"[ERROR] Unexpected response format: {data}")
                return '{"metric": "total_sales", "dimensions": [], "date_range": "current_financial_year"}'
                
        except Exception as e:
            print(f"[ERROR] WatsonX API Exception: {str(e)}")
            return '{"metric": "total_sales", "dimensions": [], "date_range": "current_financial_year"}'


# def run_query(user_query: str):
#     """
#     Main query execution flow with multi-query support.
    
#     ENHANCED FEATURES:
#     1. Detects multiple separate queries (e.g., "wave city and wave estate")
#     2. Executes each query separately
#     3. Returns combined results
#     """
    
#     # Initialize WatsonX model with auto token management
#     model = WatsonxChatModel(
#         project_id=os.getenv("WATSONX_PROJECT_ID"),
#         api_key=os.getenv("WATSONX_API_KEY"),
#         model_id=os.getenv("WATSONX_MODEL_ID", "openai/gpt-oss-120b")
#     )
    
#     # Convert NL → SemanticIntent(s)
#     adapter = WatsonxSemanticAdapter(model)
    
#     # Check if this is a multi-query
#     intents = adapter._detect_multi_query(user_query)
    
#     # Semantic → SQL → Presto execution
#     orchestrator = SemanticOrchestrator("semantic/model")
    
#     # if len(intents) > 1:
#     #     # Multiple separate queries
#     #     print(f"\n[APP] Detected {len(intents)} separate queries")
#     #     results = orchestrator.execute_multiple_intents(intents)
        
#     #     # Return combined results
#     #     return {
#     #         "question": user_query,
#     #         "multi_query": True,
#     #         "results": results
#     #     }
#     # else:
#     #     # Single query
#     #     intent = intents[0]
#     #     result = orchestrator.execute_intent(intent)
        
#     #     return {
#     #         "question": user_query,
#     #         "multi_query": False,
#     #         "sql": result["sql"],
#     #         "warnings": [str(w) for w in result["warnings"]],
#     #         "columns": result["columns"],
#     #         "rows": result["rows"],
#     #     }

#     if len(intents) > 1:
#     # Multiple separate queries
#         print(f"\n[APP] Detected {len(intents)} separate queries")
#         results = orchestrator.execute_multiple_intents(intents)
        
#         # Check if this is a comparison query
#         is_comparison = any(keyword in user_query.lower() for keyword in ['vs', 'v/s', 'versus', 'v.s'])
        
#         response = {
#             "question": user_query,
#             "multi_query": True,
#             "is_comparison": is_comparison,
#             "results": results
#         }
    
#     # # Add comparison summary for easy viewing
#     # if is_comparison and len(results) == 2:
#     #     response["comparison_summary"] = {
#     #         "item_1": {
#     #             "label": results[0]["query"],
#     #             "value": results[0]["rows"][0][0] if results[0]["rows"] else 0
#     #         },
#     #         "item_2": {
#     #             "label": results[1]["query"],
#     #             "value": results[1]["rows"][0][0] if results[1]["rows"] else 0
#     #         },
#     #         "difference": (results[0]["rows"][0][0] if results[0]["rows"] else 0) - 
#     #                      (results[1]["rows"][0][0] if results[1]["rows"] else 0),
#     #         "percentage_change": round(
#     #             ((results[0]["rows"][0][0] - results[1]["rows"][0][0]) / 
#     #              results[1]["rows"][0][0] * 100) if results[1]["rows"] and results[1]["rows"][0][0] != 0 else 0,
#     #             2
#     #         )
#     #     }
    

#     # Add comparison summary for easy viewing
#     if is_comparison and len(results) == 2:
#         # Safely extract numeric values
#         def get_numeric_value(result):
#             """Safely extract first numeric value from result."""
#             try:
#                 if result.get("rows") and len(result["rows"]) > 0:
#                     val = result["rows"][0][0]
#                     # Convert to float if it's a string number
#                     if isinstance(val, str):
#                         val = val.replace(',', '')
#                         return float(val)
#                     return float(val) if val is not None else 0
#                 return 0
#             except (ValueError, TypeError, IndexError):
#                 return 0
        
#         value_1 = get_numeric_value(results[0])
#         value_2 = get_numeric_value(results[1])
        
#         # Calculate difference and percentage
#         difference = value_1 - value_2
#         percentage_change = 0
#         if value_2 != 0:
#             percentage_change = round((difference / value_2) * 100, 2)
        
#         response["comparison_summary"] = {
#             "item_1": {
#                 "label": results[0]["query"],
#                 "value": value_1
#             },
#             "item_2": {
#                 "label": results[1]["query"],
#                 "value": value_2
#             },
#             "difference": difference,
#             "percentage_change": percentage_change,
#             "interpretation": f"{'Increase' if difference > 0 else 'Decrease' if difference < 0 else 'No change'} of {abs(difference)} ({abs(percentage_change)}%)"
#         }

    
#     return response





def run_query(user_query: str):
    """
    Main query execution flow with multi-query and comparison support.
    """
    
    # Initialize WatsonX model
    model = WatsonxChatModel(
        project_id=os.getenv("WATSONX_PROJECT_ID"),
        api_key=os.getenv("WATSONX_API_KEY"),
        model_id=os.getenv("WATSONX_MODEL_ID", "openai/gpt-oss-120b")
    )
    
    # Convert NL → SemanticIntent(s)
    adapter = WatsonxSemanticAdapter(model)
    
    # Check if this is a multi-query
    intents = adapter._detect_multi_query(user_query)
    
    # Get comparison info if it exists
    comparison = adapter._detect_comparison_query(user_query)
    
    # Semantic → SQL → Presto execution
    orchestrator = SemanticOrchestrator("semantic/model")
    
    if len(intents) > 1:
        # Multiple separate queries
        print(f"\n[APP] Detected {len(intents)} separate queries")
        results = orchestrator.execute_multiple_intents(intents)
        
        # Check if this is a comparison query
        is_comparison = any(keyword in user_query.lower() for keyword in ['vs', 'v/s', 'versus', 'v.s'])
        
        response = {
            "question": user_query,
            "multi_query": True,
            "is_comparison": is_comparison,
            "results": results
        }
        
        # Add comparison summary
        if is_comparison and len(results) == 2:
            def get_numeric_value(result):
                """Safely extract first numeric value from result."""
                try:
                    if result.get("rows") and len(result["rows"]) > 0:
                        row = result["rows"][0]
                        
                        # Try each value in the row
                        for val in row:
                            if val is None:
                                continue
                            
                            if isinstance(val, (int, float)):
                                return float(val)
                            
                            if isinstance(val, str):
                                # Skip text columns
                                if any(char.isalpha() for char in val.replace(',', '').replace('.', '').replace('-', '')):
                                    continue
                                
                                try:
                                    val_clean = val.replace(',', '').strip()
                                    return float(val_clean)
                                except ValueError:
                                    continue
                        
                        return 0
                    return 0
                except (ValueError, TypeError, IndexError) as e:
                    print(f"[ERROR] Failed to extract numeric value: {e}")
                    return 0
            
            value_1 = get_numeric_value(results[0])
            value_2 = get_numeric_value(results[1])
            
            # Calculate difference and percentage
            difference = value_1 - value_2
            percentage_change = 0
            if value_2 != 0:
                percentage_change = round((difference / value_2) * 100, 2)
            
            # Get labels from comparison object if available
            if comparison and comparison.get('labels'):
                label_1 = comparison['labels'][0]
                label_2 = comparison['labels'][1]
            else:
                label_1 = results[0].get("query", "Item 1")
                label_2 = results[1].get("query", "Item 2")
            
            response["comparison_summary"] = {
                "item_1": {
                    "label": label_1,
                    "value": value_1
                },
                "item_2": {
                    "label": label_2,
                    "value": value_2
                },
                "interpretation": f"{'Increase' if difference > 0 else 'Decrease' if difference < 0 else 'No change'} of {abs(difference)} ({abs(percentage_change)}%)"
            }
        
        return response
    else:
        # Single query
        intent = intents[0]
        result = orchestrator.execute_intent(intent)
        
        return {
            "question": user_query,
            "multi_query": False,
            "sql": result["sql"],
            "warnings": [str(w) for w in result["warnings"]],
            "columns": result["columns"],
            "rows": result["rows"],
        }