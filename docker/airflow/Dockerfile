FROM apache/airflow:3.3.2

# 공식 이미지에 amazon provider(boto3)와 requests가 이미 들어 있어 의존성은 추가로 깔리지 않는다.
# 개발 중에는 compose가 ./src를 마운트하고 PYTHONPATH로 읽으므로 코드 수정 시 재빌드가 필요 없다.
# 이 이미지에도 패키지를 설치해 두는 이유: 나중에 k8s로 옮길 때 이미지 하나로 실행하기 위함이다.
COPY --chown=airflow:root pyproject.toml /opt/ddareungi/pyproject.toml
COPY --chown=airflow:root src /opt/ddareungi/src
RUN pip install --no-cache-dir /opt/ddareungi
