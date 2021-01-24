class PerformanceCounters:
    
    def __init__(self, start_time, cpu_utilization = None, network_in = None, network_out = None, ebs_write_bytes = None, ebs_read_bytes = None, \
        disk_write_ops = None, disk_read_ops = None, disk_write_bytes = None, disk_read_bytes = None, is_idle = None, cost = None):

        self.start_time = start_time
        self.cpu_utilization = cpu_utilization
        self.network_in = network_in
        self.network_out = network_out       
        self.ebs_write_bytes = ebs_write_bytes
        self.ebs_read_bytes = ebs_read_bytes
        self.disk_read_bytes = disk_read_bytes
        self.disk_write_bytes = disk_write_bytes
        self.disk_write_ops = disk_write_ops
        self.disk_read_ops = disk_read_ops        
        self.is_idle = is_idle
        self.cost = cost
       