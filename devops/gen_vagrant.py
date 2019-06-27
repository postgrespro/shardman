#!/usr/bin/env python3
# lol

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Generate Vagrantfile and inventory_manual/manual for given num of nodes.
    """,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', dest='numnodes', default=4, type=int, help='number of nodes')
    parser.add_argument('--vboxname-prefix', dest='vboxname_prefix', default="node", type=str,
                        help="Virtual box names prefix")
    parser.add_argument('-i', dest='first_ip_last_byte', default=11, type=int, help='last byte of ip of first node. ips are assigned skipping 10 addresses, e.g. .11, .21, etc')
    parser.add_argument('--network', dest='network', default="10.42.42", type=str, help='/24 network')
    args = parser.parse_args()

    numnodes = args.numnodes
    vboxname_prefix = args.vboxname_prefix
    first_ip_last_byte = args.first_ip_last_byte
    network = args.network

    with open('Vagrantfile', 'w+') as vf:
        vf.write('Vagrant.configure("2") do |config|\n')
        for i in range(numnodes):
            nodenum = i + 1
            nodename = "{}{}".format(vboxname_prefix, nodenum)
            ip_last_byte = first_ip_last_byte + i * 10
            ip = "{}.{}".format(network, ip_last_byte)
            hostname = "vg{}".format(nodenum)

            vf.write("""  config.vm.define "{}" do |node|
    node.vm.box = "ubuntu/xenial64"
    node.vm.network "private_network", ip: "{}"
    node.vm.hostname = "{}"
  end

""".format(nodename, ip, hostname))

        vf.write("""  config.vm.provision "shell" do |s|
    ssh_pub_key = File.readlines("#{Dir.home}/.ssh/id_rsa.pub").first.strip
    s.inline = <<-SHELL
      echo #{ssh_pub_key} >> /home/ubuntu/.ssh/authorized_keys
      echo #{ssh_pub_key} >> /root/.ssh/authorized_keys
    SHELL
  end

  config.vm.provision "shell" do |s|
    s.inline = <<-SHELL
""")

        for i in range(numnodes):
            nodenum = i + 1
            ip_last_byte = first_ip_last_byte + i * 10
            ip = "{}.{}".format(network, ip_last_byte)
            hostname = "vg{}".format(nodenum)
            vf.write("""      echo "{} {}" >> /etc/hosts\n""".format(ip, hostname))

        vf.write("""    SHELL
  end
end
""")

    with open('inventory_manual/manual', 'w+') as inv_f:
        inv_f.write("""# For local development

[nodes:vars]
ansible_user=ubuntu

[nodes]
""")
        for i in range(numnodes):
            ip_last_byte = first_ip_last_byte + i * 10
            ip = "{}.{}".format(network, ip_last_byte)
            inv_f.write("""{}\n""".format(ip))

        inv_f.write("""
[etcd_nodes]
""")
        for i in range(min(numnodes, 3)):
            ip_last_byte = first_ip_last_byte + i * 10
            ip = "{}.{}".format(network, ip_last_byte)
            inv_f.write("""{}\n""".format(ip))
