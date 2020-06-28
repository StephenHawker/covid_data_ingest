"""SSH connection, commands, admin functions """
import logging
import json
import paramiko

class SSHInstanceAdmin:
    ############################################################
    # constructor
    ############################################################
    def __init__(self, keypair_file, ip_address):
        self.name = "SSH Instance Management"
        self.LOGGER = logging.getLogger(__name__)
        self.keypair_file = keypair_file
        self.ip_address = ip_address

        # Get the key pair, save it in key
        self.key = paramiko.RSAKey.from_private_key_file(keypair_file)  # 'convex-ec2-keypair.pem'
        self.client = paramiko.SSHClient()

    ############################################################
    # Connect to instance via SSH
    ############################################################
    def ssh_connect(self):
        """
        Connect via ssh to the instance
        Usage is shown in usage_demo at the end of this module.
        :param keypair_file: The keypair file to use
        :param ip_address: IP address of instance to connect to
        """

        try:

            # Auto add policy for new host
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect/ssh to an instance
            # Here 'ec2-user' is user name and 'instance_ip' is public IP of EC2
            self.client.connect(hostname=self.instance_ip, username="ec2-user", pkey=self.key, timeout=100)

            self.transport = self.client.get_transport()
            self.session = self.transport.open_session()

        except Exception as error:

            self.LOGGER.exception("Couldn't connect to ec2 instance ip address:  %s", ip_address)
            self.LOGGER.exception("Error:  %s", repr(error))
            raise error(repr(error))

    ############################################################
    # Execute commands
    ############################################################
    def execute_commands(self, command_list):
        """
        Execute commands against the connected instance
        :param command_list: The list of commands to execute
        """

        try:

            # Execute a command(cmd) after connecting/ssh to an instance
            for command in command_list:
                stdin, stdout, stderr = self.client.exec_command(command)

                output = stdout.readlines()
                stdinput = stdin.readlines()
                erroroutput = stderr.readlines()

        except Exception as error:

            self.LOGGER.exception("Error executing command to %s", command_list)
            self.LOGGER.exception("Error:  %s", repr(error))
            raise error(repr(error))

    ############################################################
    # Close the connection
    ############################################################
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        context manager for close connection
        :param exc_type:
        :param exc_value:
        :param exc_traceback:
        """

        try:
            # close the client connection once the job is done
            self.client.close()

        except Exception as error:

            self.LOGGER.exception("Error closing connection to %s", self.ip_address)
            self.LOGGER.exception("Error:  %s", repr(error))
            raise error(repr(error))