

include_recipe "apt"

package "openjdk-7-jre-headless"

network = node["base"]["network"]
adapter = network["adapter"]

bash "configure_network_qdisc_rules" do
  code <<-EOF
  tc qdisc add dev #{adapter} root netem
  tc qdisc change dev #{adapter} root netem delay #{network["delay"]} loss #{network["packet_loss"]}
  EOF
end

