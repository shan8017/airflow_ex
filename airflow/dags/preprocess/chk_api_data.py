
__all__ = ["chk_api_data"]

def chk_api_data(ti):
    data = ti.xcom_pull(task_ids=["crawl_naver"])
    if len(data[0]) <= 10:
        print(data)
        event_data = {
                'message' : '이상 이벤트 : 데이터의 길이가 10 이하입니다.',
                'data_length': len(data[0])
                }
        create_event(event_data)
    else:
        print(data)


def create_event(event_data):
    """
    Creates an event using the provided event data.

    Parameters:
    - event_data (dict): A dictionary containing the event data.

    Example event_data:
    {
        'message': 'Event message',
        'data_length': 5,  # or any other relevant data
        # Any other key-value pairs as needed
    }
    """
    # Here you would implement the logic to create and save the event
    # This could involve inserting the event data into a database, logging it, or sending it to a monitoring system
    # For demonstration purposes, let's just print the event data
    print("Event created:", event_data)        
