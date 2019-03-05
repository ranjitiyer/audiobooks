rm skill.zip

cp -R src lib/python3.6/site-packages/

pushd .

cd lib/python3.6/site-packages/

zip -r -u skill.zip *

popd 

mv lib/python3.6/site-packages/skill.zip .

rm -R lib/python3.6/site-packages/src/

# Verify the source is in there
unzip -l skill.zip | grep src

# update Lambda
AWS_PROFILE=ranjitiyer aws lambda update-function-code \
	--function-name helloWorldAlexa \
	--zip-file fileb://skill.zip \
	--region us-east-1
