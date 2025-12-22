import pendulum

def skip_at_kst_21(**context) -> bool:
    now = pendulum.now("Asia/Seoul")
    return now.hour != 21