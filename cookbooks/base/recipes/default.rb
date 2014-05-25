

include_recipe "apt"
include_recipe "java"

network = node["base"]["network"]
adapter = network["adapter"]

bash "configure_network_qdisc_rules" do
  code <<-EOF
  tc qdisc add dev #{adapter} root netem
  tc qdisc change dev #{adapter} root netem delay #{network["delay"]} loss #{network["packet_loss"]}
  EOF
end

