
# import datetime
# import logging
# import azure.functions as func
# import requests
# app = func.FunctionApp()

# @app.function_name(name="mytimer")
# @app.schedule(schedule="0 35 17 * * *", 
#               arg_name="mytimer",
#               run_on_startup=True) 
# def test_function(mytimer: func.TimerRequest) -> None:
#     utc_timestamp = datetime.datetime.utcnow().replace(
#         tzinfo=datetime.timezone.utc).isoformat()
#     if mytimer.past_due:
#         logging.info('The timer is past due!')
#     response = requests.get("https://detention-api.azurewebsites.net/test")
    
    
    
# import logging
# import azure.functions as func
# import requests
# app = func.FunctionApp()

# @app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True,
#               use_monitor=False) 
# def bbb(myTimer: func.TimerRequest) -> None:
#     if myTimer.past_due:
#         logging.info('The timer is past due!')
#     response = requests.get("https://detention-api.azurewebsites.net/test")
#     logging.info('Python timer trigger fwunction executed.')


import datetime
import logging
import azure.functions as func
import requests
app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.schedule(schedule="0 20 13 * * *", 
              arg_name="mytimer",
              run_on_startup=True) 
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    username = "cbm"
    password = "cbm"

    token_url = "https://smartmaintenance.azurewebsites.net/token"
    token_response = requests.post(
        token_url,
        data={"username": username, "password": password},
    )
    token_data = token_response.json()
    access_token = token_data["access_token"]

    predict_url = "https://smartmaintenance.azurewebsites.net/forecast-14days"
    headers = {"Authorization": f"Bearer {access_token}"}

  

    prediction_response = requests.post(predict_url, headers=headers) #, json=input_data
