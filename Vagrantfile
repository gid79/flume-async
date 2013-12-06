# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "precise64"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  (0.upto(3)).each do |idx|
    config.vm.define "host-#{idx}" do |cfg|
      cfg.vm.network :private_network, ip: "192.168.33.#{10+idx}"
      cfg.vm.provision :chef_solo do |chef|
        chef.cookbooks_path = "./cookbooks"
        chef.add_recipe "base"        
        chef.add_recipe "server" if idx > 0


        chef.json = { idx:idx }
      end
    end
  end
end
