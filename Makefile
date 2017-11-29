install:
	sudo pip install -r requirements.txt .

upgrade:
	sudo pip install --upgrade -r requirements.txt .

spec:
	utils/mk-spec.py > mapzen/whosonfirst/brands/spec.py

