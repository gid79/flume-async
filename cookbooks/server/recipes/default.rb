

template "/etc/init/performance-endpoint.conf" do
  source "performance-endpoint.conf.erb"
  mode "0644"
  variables params
  notifies :restart, "service[performance-endpoint]" 
end

service "performance-endpoint" do
  supports :restart => false, :reload => true, :status => true
  restart_command "/sbin/stop performance-endpoint; /sbin/start performance-endpoint"
  action :start
  provider Chef::Provider::Service::Upstart
end
