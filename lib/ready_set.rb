# lib/ready_set.rb

require 'ready_set/configuration'
require 'ready_set/default_resolver'
require 'ready_set/middleware'
require 'ready_set/query'
require 'ready_set/railtie' if defined?(Rails::Railtie)
require 'ready_set/status'
require 'ready_set/table'

require 'active_record'

module ReadySet
  attr_writer :configuration

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure
    yield(configuration)
  end

  def self.current_config
    configuration.inspect
  end

  # Returns ReadySet's status.
  #
  # @return [ReadySet::Status]
  def self.status
    ReadySet::Status.fetch
  end

  # Returns a list of all the tables known by ReadySet along with their statuses.
  #
  # @return [Array<ReadySet::Table>]
  def self.tables
    ReadySet::Table.all
  end

  # Executes a raw SQL query against ReadySet. The query is sanitized prior to being executed.
  #
  # @param [Array<Object>] *sql_array the SQL array to be executed against ReadySet
  # @return [PG::Result]
  def self.raw_query(*sql_array)
    ActiveRecord::Base.establish_connection(ReadySet.configuration.connection_url)
    ActiveRecord::Base.connection.execute(ActiveRecord::Base.sanitize_sql_array(sql_array))
  end
end
