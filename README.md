📊 교사용 통합 대시보드 (Streamlit)

학생들이 제출한 DAT1, DAT2, DAT3 과제 결과를 교사가 한눈에 확인하고 관리할 수 있도록 제작된 Streamlit 대시보드입니다.

✨ 주요 기능

통합 조회: 여러 데이터베이스 스키마에 분산된 DAT1, DAT2, DAT3 테이블을 하나의 화면에서 모두 조회합니다.

자동 감지: 테이블의 answerN, feedbackN 컬럼을 자동으로 분석하여 문항 수를 감지합니다.

상세 분석: JSON 형식으로 저장된 채점 결과(점수, 성취 수준, 피드백 등)를 가독성 높은 표와 상세 보기 탭으로 제공합니다.

검색 및 다운로드: 학번, 답안, 피드백 등 키워드로 제출 내역을 검색하고, 전체 결과를 CSV 파일로 내려받을 수 있습니다.

🚀 실행 가이드

이 프로젝트를 로컬 환경에서 실행하거나 Streamlit Share에 배포하는 방법입니다.

1️⃣ 라이브러리 설치
프로젝트 실행에 필요한 라이브러리들을 설치합니다.

pip install -r requirements.txt

2️⃣ 데이터베이스 연결 설정
Streamlit의 Secrets management 기능을 사용하여 데이터베이스 연결 정보를 안전하게 관리합니다. 프로젝트 루트에 .streamlit 폴더를 만들고, 그 안에 secrets.toml 파일을 아래 형식으로 작성하세요.

# .streamlit/secrets.toml

[connections.mysql]
host = "YOUR_DATABASE_HOST"       # 예: aws-rds-endpoint.ap-northeast-2.rds.amazonaws.com
port = 3306
database = "YOUR_DATABASE_NAME"    # 예: pr
user = "YOUR_USERNAME"
password = "YOUR_PASSWORD"
dialect = "mysql"

3️⃣ Streamlit 앱 실행
터미널에서 아래 명령어를 입력하여 앱을 실행합니다.

streamlit run teacher_dashboard_fixed.py

☁️ Streamlit Share 배포
이 GitHub 저장소를 Streamlit Share에 연결하여 웹에 배포할 수 있습니다.

⚠️ 중요: 배포 시, 위에서 작성한 secrets.toml 파일의 내용을 Streamlit Share의 Secrets 설정에 그대로 복사하여 붙여넣어야 합니다.
