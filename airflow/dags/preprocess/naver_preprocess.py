from pandas import json_normalize

__all__ = ["preprocessing"]

def preprocessing(ti):
    search_result = ti.xcom_pull(task_ids=["crawl_naver"])
    if not len(search_result):
        raise valueError("검색결과 없음")

    items = search_result[0]["items"]
    processed_items = json_normalize([
            {"title": item["title"],
             "address": item["address"],
             "category": item["category"],
             "description": item["description"],
             "link":item["link"]} for item in items
            ])
    
    processed_items.to_csv("/home/shan/airflow/dags/data/naver_processed_result.csv",index=None, header=False)

