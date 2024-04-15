from flask import Flask,render_template,request,redirect,url_for, jsonify
import boto3
import uuid
import random
import time
import logging
from logging.handlers import RotatingFileHandler

# Configure logging

# Create a Flask application instance
app = Flask(__name__)

logging.basicConfig(level=logging.INFO)


formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_file = "flask.log"  # Specify the log file path
file_handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=10)
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)

s3 = boto3.client('s3',
                  aws_access_key_id='Yourkeyid',
                  aws_secret_access_key='youraccesskey')
@app.route('/')
def index():
    return render_template('index.html')

# Define a route
@app.route('/dataRequestForm',methods=['POST'])
def hello_world():
    job_uuid=str(uuid.uuid4())
    state=request.form['state']
       
    cluster_id = 'j-1ZU43AOZ8IRO'
    script_path = 's3://practiseprojcet/SparkJob.py'

    emr_client = boto3.client('emr', region_name='us-east-1')

    # Step configuration for the PySpark job
    step_args = ['sudo','spark-submit', script_path,job_uuid,state]

    # Submit the PySpark job to the EMR cluster
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'PySpark Job',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': step_args
                }
            },
        ]
    )

    return render_template('index2.html',file_id=job_uuid)

@app.route('/get-status',methods=['GET'])
def get_status():
    job_uuid = str(request.args.get('file'))

  
    response = s3.list_objects_v2(
        Bucket='practiseprojcet',
        Prefix=f"Output1/{job_uuid}/"
    )
    app.logger.info(f"The path is having id {job_uuid}")
    app.logger.info(f"Response from S3 {response}")



    # Check if the file exists
    for obj in response.get('Contents', []):
        if obj['Key'].startswith(f"Output1/{job_uuid}/part"):
            s3_file_key = obj['Key']

            app.logger.info("inside the loop")

            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': 'practiseprojcet', 'Key': s3_file_key},
                ExpiresIn=3600  # URL expiration time in seconds
            )
            app.logger.info(f"url is::  {url}")
            return jsonify({'isReady': True, 'url': url})
    
    
    return jsonify({'isReady': False})	 
         
         



# This if statement ensures that the development server is started only if this script is executed directly,
# not when it is imported as a module.
if __name__ == '__main__':
    # Run the Flask application
    app.run(debug=True)
