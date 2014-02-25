#
# Cookbook Name:: mongodb
# Definition:: mongodb
#
# Copyright 2011, edelight GmbH
# Authors:
#       Markus Korn <markus.korn@edelight.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'json'

class Chef::ResourceDefinitionList::MongoDB

  def self.connect(node)
    # lazy require, to move loading this modules to runtime of the cookbook
    require 'rubygems'
    require 'mongo'

    username = node['mongodb']['username']
    password = node['mongodb']['password']

    connection = nil
    db = nil
    begin
      rescue_connection_failure do
        connection = Mongo::MongoClient.new('localhost', node['mongodb']['config']['port'], :op_timeout => 5, :slave_ok => true)
        db = connection.db("admin")
        if auth_set?(node)
          auth = db.authenticate(username, password)
          Chef::Log.info("DB auth result #{auth} for #{username}:#{password}")
        end
      end
    rescue => e
      Chef::Log.warn("Could not connect to database: 'localhost:#{node['mongodb']['config']['port']}', reason: #{e}")
    end
    [connection, db]
  end

  def self.auth_set?(node)
    @auth_set ||= get_auth_set(node)
  end

  def self.get_auth_set(node)
    # lazy require, to move loading this modules to runtime of the cookbook
    require 'rubygems'
    require 'mongo'

    username = node['mongodb']['username']
    password = node['mongodb']['password']

    connection = Mongo::MongoClient.new('localhost', node['mongodb']['config']['port'])
    db = connection.db("admin")
    begin
      db.authenticate(username, password)
    rescue => e
      return false
    end
    return true
  end

  def self.is_master?(node)
    connection, db = connection(node)

    return false unless connection

    cmd = BSON::OrderedHash.new
    cmd['isMaster'] = 1

    begin
      result = db.command(cmd, :check_response => false)
      return result['ismaster']
    rescue Mongo::OperationTimeout
      Chef::Log.warn('Started configuring the replicaset, this will take some time, another run should run smoothly')
      return false
    end
  end

  def self.mongo_shell(node, cmd)
    username = node['mongodb']['username']
    password = node['mongodb']['password']

    auth_string = auth_set?(node) ? "-u #{username} -p #{password}" : ''
    "mongo admin #{auth_string} --eval '#{cmd}'"
  end

  def self.exec_mongo(node, cmd)
    `#{mongo_shell(node, cmd)}`
  end

  def self.rs_config(node)
    config_string = exec_mong(node, 'printjson(rs.conf())')
    config = JSON.parse(config_string.lines[2..-1].join)
  end

  def self.configure_replicaset(node, name, members)
    if members.length == 0
      if Chef::Config[:solo]
        abort("Cannot configure replicaset '#{name}', no member nodes found")
      else
        Chef::Log.warn("Cannot configure replicaset '#{name}', no member nodes found")
        return
      end
    end

    connection, db = connection(node)

    return unless connection

    Chef::Log.info(
      "Configuring replicaset with members #{members.map { |n| n['host'] }.join(', ')}"
    )

    cmd = BSON::OrderedHash.new
    cmd['replSetInitiate'] = {
      '_id' => name,
      'members' => members
    }

    begin
      result = db.command(cmd, :check_response => false)
    rescue Mongo::OperationTimeout
      Chef::Log.warn('Started configuring the replicaset, this will take some time, another run should run smoothly')
      return
    end
    if result.fetch('ok', nil) == 1
      # everything is fine, do nothing
    elsif result.fetch('errmsg', nil).match(/already initialized/)
      # check if both configs are the same
      config = rs_config(node)

      if config['_id'] == name && config['members'] == members
        # config is up-to-date, do nothing
        Chef::Log.info("Replicaset '#{name}' already configured")
      else
        # remove removed members from the replicaset and add the new ones
        max_id = config['members'].map { |member| member['_id'] }.max
        members.map! { |member| member['host'] }
        config['version'] += 1
        old_members = config['members'].map { |member| member['host'] }
        members_delete = old_members - members
        config['members'] = config['members'].delete_if { |m| members_delete.include?(m['host']) }
        config['members'].map! do |m|
          host = m['host']
          { '_id' => m['_id'], 'host' => host }
        end
        members_add = members - old_members
        members_add.each do |m|
          max_id += 1
          config['members'] << { '_id' => max_id, 'host' => m }
        end

        rs_connection = nil
        rs_db = nil
        rescue_connection_failure do
          rs_connection = Mongo::MongoReplicaSetClient.new(old_members)
          rs_db = connection.db("admin")
          if auth_set?(node)
            auth = rs_db.authenticate(username, password)
            Chef::Log.info("DB auth result #{auth} for #{username}:#{password}")
          end
        end

        cmd = BSON::OrderedHash.new
        cmd['replSetReconfig'] = config

        result = nil
        begin
          result = rs_db.command(cmd, :check_response => false)
        rescue Mongo::ConnectionFailure

          config = rs_config(node)

          # Validate configuration change
          if config['members'] == members
            Chef::Log.info("New config successfully applied: #{config.inspect}")
          else
            Chef::Log.error("Failed to apply new config. Current config: #{config.inspect} Target config #{members}")
            return
          end
        end
        Chef::Log.error("configuring replicaset returned: #{result.inspect}") unless result.nil? || result.fetch('errmsg', nil).nil?
      end
    elsif !result.fetch('errmsg', nil).nil?
      Chef::Log.error("Failed to configure replicaset, reason: #{result.inspect}")
    end
  end

  def self.configure_shards(node, shard_nodes)
    # lazy require, to move loading this modules to runtime of the cookbook
    require 'rubygems'
    require 'mongo'

    shard_groups = Hash.new { |h, k| h[k] = [] }

    shard_nodes.each do |n|
      if n['recipes'].include?('mongodb::replicaset')
        # do not include hidden members when calling addShard
        # see https://jira.mongodb.org/browse/SERVER-9882
        next if n['mongodb']['replica_hidden']
        key = "rs_#{n['mongodb']['shard_name']}"
      else
        key = '_single'
      end
      shard_groups[key] << "#{n['fqdn']}:#{n['mongodb']['config']['port']}"
    end
    Chef::Log.info(shard_groups.inspect)

    shard_members = []
    shard_groups.each do |name, members|
      if name == '_single'
        shard_members += members
      else
        shard_members << "#{name}/#{members.join(',')}"
      end
    end
    Chef::Log.info(shard_members.inspect)

    begin
      connection = Mongo::Connection.new('localhost', node['mongodb']['config']['port'], :op_timeout => 5)
    rescue => e
      Chef::Log.warn("Could not connect to database: 'localhost:#{node['mongodb']['config']['port']}', reason #{e}")
      return
    end

    admin = connection['admin']

    shard_members.each do |shard|
      cmd = BSON::OrderedHash.new
      cmd['addShard'] = shard
      begin
        result = admin.command(cmd, :check_response => false)
      rescue Mongo::OperationTimeout
        result = "Adding shard '#{shard}' timed out, run the recipe again to check the result"
      end
      Chef::Log.info(result.inspect)
    end
  end

  def self.configure_sharded_collections(node, sharded_collections)
    if sharded_collections.nil? || sharded_collections.empty?
      Chef::Log.warn('No sharded collections configured, doing nothing')
      return
    end

    # lazy require, to move loading this modules to runtime of the cookbook
    require 'rubygems'
    require 'mongo'

    begin
      connection = Mongo::Connection.new('localhost', node['mongodb']['config']['port'], :op_timeout => 5)
    rescue => e
      Chef::Log.warn("Could not connect to database: 'localhost:#{node['mongodb']['config']['port']}', reason #{e}")
      return
    end

    admin = connection['admin']

    databases = sharded_collections.keys.map { |x| x.split('.').first }.uniq
    Chef::Log.info("enable sharding for these databases: '#{databases.inspect}'")

    databases.each do |db_name|
      cmd = BSON::OrderedHash.new
      cmd['enablesharding'] = db_name
      begin
        result = admin.command(cmd, :check_response => false)
      rescue Mongo::OperationTimeout
        result = "enable sharding for '#{db_name}' timed out, run the recipe again to check the result"
      end
      if result['ok'] == 0
        # some error
        errmsg = result.fetch('errmsg')
        if errmsg == 'already enabled'
          Chef::Log.info("Sharding is already enabled for database '#{db_name}', doing nothing")
        else
          Chef::Log.error("Failed to enable sharding for database #{db_name}, result was: #{result.inspect}")
        end
      else
        # success
        Chef::Log.info("Enabled sharding for database '#{db_name}'")
      end
    end

    sharded_collections.each do |name, key|
      cmd = BSON::OrderedHash.new
      cmd['shardcollection'] = name
      cmd['key'] = { key => 1 }
      begin
        result = admin.command(cmd, :check_response => false)
      rescue Mongo::OperationTimeout
        result = "sharding '#{name}' on key '#{key}' timed out, run the recipe again to check the result"
      end
      if result['ok'] == 0
        # some error
        errmsg = result.fetch('errmsg')
        if errmsg == 'already sharded'
          Chef::Log.info("Sharding is already configured for collection '#{name}', doing nothing")
        else
          Chef::Log.error("Failed to shard collection #{name}, result was: #{result.inspect}")
        end
      else
        # success
        Chef::Log.info("Sharding for collection '#{result['collectionsharded']}' enabled")
      end
    end
  end

  # Ensure retry upon failure
  def self.rescue_connection_failure(max_retries = 30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      retries += 1
      raise ex if retries > max_retries
      sleep(0.5)
      retry
    end
  end
end
