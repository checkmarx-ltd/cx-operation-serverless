class EC2:    
    """
    This class holds ec2 machine properties
    """

    def __init__(self, availability_zone, instance_id, instance_type, launch_time, state, ebs_optimized, account_number, pu, account_name):
        self.availability_zone = availability_zone
        self.instance_id = instance_id
        self.instance_type = instance_type
        self.launch_time = launch_time
        self.state = state
        self.ebs_optimized = ebs_optimized
        self.performance_counters_list = []
        self.account_number = account_number
        self.pu = pu
        self.account_name = account_name