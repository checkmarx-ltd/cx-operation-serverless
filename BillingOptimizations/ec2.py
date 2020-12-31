class EC2:    
    """
    This class holds ec2 machine properties
    """

    def __init__(self, availability_zone, instance_id, instance_type, launch_time, state, ebs_optimized, tags, instance_owner_id ):
        self.availability_zone = availability_zone
        self.instance_id = instance_id
        self.instance_type = instance_type
        self.launch_time = launch_time
        self.state = state
        self.ebs_optimized = ebs_optimized
        self.tags = tags              
        self.performance_counters_list = []
        self.instance_owner_id = instance_owner_id