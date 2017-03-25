import boto3


def create_instance_from_snapshot(
        region,
        db_instance_identifier,
        db_snapshot_identifier,
        db_instance_class
        ):
    rds = boto3.client('rds', region)
    response = rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier = db_instance_identifier,
        DBSnapshotIdentifier = db_snapshot_identifier,
        DBInstanceClass = db_instance_class,
        MultiAZ = False,
        PubliclyAccessible = True,
        AutoMinorVersionUpgrade = True,
        )


def delete_instance(region, db_instance_identifier):
    rds = boto3.client('rds', region)
    response = rds.delete_db_instance(
        DBInstanceIdentifier = db_instance_identifier,
        SkipFinalSnapshot = True
        )


def get_instances(region):
    rds = boto3.client('rds', region)
    return rds.describe_db_instances()['DBInstances']


def get_instance_status(region, db_instance_identifier):
    rds = boto3.client('rds', region)
    response = rds.describe_db_instances(
        DBInstanceIdentifier = db_instance_identifier
        )
    return response['DBInstances'][0]['DBInstanceStatus']


def modify_vpc_security_groups(
        region,
        db_instance_identifier,
        vpc_security_group_ids
        ):
    rds = boto3.client('rds', region)
    response = rds.modify_db_instance(
        DBInstanceIdentifier = db_instance_identifier,
        VpcSecurityGroupIds = vpc_security_group_ids,
        ApplyImmediately = True
        )


