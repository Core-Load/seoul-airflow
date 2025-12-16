"""
서울 공공 데이터 API 및 AWS 연동 관련 플러그인
"""

from airflow.plugins_manager import AirflowPlugin

class SeoulPlugin(AirflowPlugin):
    name = "seoul_plugin"
