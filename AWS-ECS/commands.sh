https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-cli-tutorial-fargate.html

ecs-cli configure profile --profile-name covid19 --access-key AKIAJGC2SFF24ZXQD2DQ --secret-key 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

ecs-cli configure --cluster covid19 --default-launch-type FARGATE  --region us-east-2 --config-name covid19_config


aws iam --region us-east-2 create-role --role-name ecsTaskExecutionRole --assume-role-policy-document file://task-execution-assume-role.json

{
    "Role": {
        "Path": "/",
        "RoleName": "ecsTaskExecutionRole",
        "RoleId": "AROAZY7OCKH2OUYOBW264",
        "Arn": "arn:aws:iam::672124785140:role/ecsTaskExecutionRole",
        "CreateDate": "2020-04-01T14:56:45+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ecs-tasks.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
    }
}


aws iam --region us-east-2 attach-role-policy --role-name ecsTaskExecutionRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

ecs-cli up --keypair /Users/steve/.ssh/pk-APKAINJUINOCF5XXKRDQ.pem --size 2 --instance-type t2.micro --cluster-config covid19_config --ecs-profile covid19

VPC created: vpc-0bd0516f02ec32b8c
Subnet created: subnet-01960df622a6da7e3
Subnet created: subnet-0ed3f7937ba9b59bc


aws ec2 describe-security-groups --filters Name=vpc-id,Values=vpc-0bd0516f02ec32b8c --region us-east-2

{
    "SecurityGroups": [
        {
            "Description": "default VPC security group",
            "GroupName": "default",
            "IpPermissions": [
                {
                    "IpProtocol": "-1",
                    "IpRanges": [],
                    "Ipv6Ranges": [],
                    "PrefixListIds": [],
                    "UserIdGroupPairs": [
                        {
                            "GroupId": "sg-0d28a1fd4c7db5558",
                            "UserId": "672124785140"
                        }
                    ]
                }
            ],
            "OwnerId": "672124785140",
            "GroupId": "sg-0d28a1fd4c7db5558",
            "IpPermissionsEgress": [
                {
                    "IpProtocol": "-1",
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0"
                        }
                    ],
                    "Ipv6Ranges": [],
                    "PrefixListIds": [],
                    "UserIdGroupPairs": []
                }
            ],
            "VpcId": "vpc-0bd0516f02ec32b8c"
        }
    ]
}

aws ec2 authorize-security-group-ingress --group-id sg-0d28a1fd4c7db5558 --protocol tcp --port 80 --cidr 0.0.0.0/0 --region us-east-2

aws ec2 authorize-security-group-ingress --group-id sg-0d28a1fd4c7db5558 --protocol tcp --port 9200 --cidr 0.0.0.0/0 --region us-east-2

aws ec2 authorize-security-group-ingress --group-id sg-0d28a1fd4c7db5558 --protocol tcp --port 9300 --cidr 0.0.0.0/0 --region us-east-2


#create containers
ecs-cli compose  --project-name covid19 up --create-log-groups --ecs-profile covid19 --cluster-config covid19_config

#create services
ecs-cli compose --project-name covid19 service up --create-log-groups --ecs-profile covid19 --cluster-config covid19_config

ecs-cli compose --project-name covid19 service ps --cluster-config covid19_config --ecs-profile covid19

ecs-cli compose --project-name covid19  ps --cluster-config covid19_config --ecs-profile covid19


ecs-cli logs --task-id 29efa82e-b20f-4f0c-b626-151d1cebbb74


ecs-cli compose --project-name covid19 service down --cluster-config covid19_config --ecs-profile covid19


ecs-cli compose --project-name covid19  down --cluster-config covid19_config --ecs-profile covid19




ecs-cli down --force --cluster-config covid19_config --ecs-profile covid19




EC2 INSTANCES:

chmod 400 /Users/steve/.ssh/ec2-covid.pem
ssh -i "/Users/steve/.ssh/ec2-covid.pem" ec2-user@ec2-18-222-166-250.us-east-2.compute.amazonaws.com


#mount partition
lsblk
sudo file -s xvdb
sudo mkfs -t xfs /dev/xvdb
sudo mount /dev/xvdb /data