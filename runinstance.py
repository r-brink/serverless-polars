import os

import boto3
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Connect to the EC2 service
ec2 = boto3.client("ec2")

# Define script to run at instance start
# wget execute.py from location or clone from git repository
user_data = f"""#!/bin/bash
    yum install -y python3-pip
    yum install -y git
    pip3 install polars boto3
    wget [location]/execute.py -P /home/ec2-user/
    python3 /home/ec2-user/execute.py
    """

# Create a new EC2 instance
response = ec2.run_instances(
    ImageId="ami-06c39ed6b42908a36",
    BlockDeviceMappings=[
        {
            "DeviceName": "/dev/xvda",
            "Ebs": {"DeleteOnTermination": True, "VolumeSize": 25},
        }
    ],
    InstanceType=os.environ.get("INSTANCETYPE"),
    MinCount=1,
    MaxCount=1,
    SecurityGroupIds=[os.environ.get("SECURITY_GROUP_ID")],
    KeyName=os.environ.get("KEYPAIR"),
    UserData=user_data,
)

# Get the ID of the new instance
instance_id = response["Instances"][0]["InstanceId"]

# Wait for the instance to be in the running state
waiter = ec2.get_waiter("instance_running")
waiter.wait(InstanceIds=[instance_id])

# Print the public IP address of the new instance
instance = ec2.describe_instances(InstanceIds=[instance_id])
public_ip = instance["Reservations"][0]["Instances"][0]["PublicIpAddress"]
print(f"The public IP address of the new instance is: {public_ip}")

# Terminating the instance needs improvement when working with larger files
# as it often terminates before query ends.
# If you remove DO NOT FORGET to manually terminate the instance in the AWS portal.

# Terminate the instance
ec2.terminate_instances(InstanceIds=[instance_id])

# Wait for the instance to be terminated
waiter = ec2.get_waiter("instance_terminated")
waiter.wait(InstanceIds=[instance_id])

print(f"Instance {instance_id} has been terminated")
