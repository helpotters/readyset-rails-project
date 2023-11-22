require 'active_model'

module ReadySet
  # Represents ReadySet's current status.
  class Status
    include ActiveModel::AttributeMethods

    attr_reader :connection_count, :controller_status, :last_completed_snapshot,
      :last_replicator_error, :last_started_controller, :last_started_replication,
      :minimum_replication_offset, :maximum_replication_offset

    # Returns a list of all the tables known by ReadySet along with their statuses. This
    # information is retrieved by invoking `SHOW READYSET STATUS` on ReadySet.
    #
    # @return [Array<ReadySet::Table>]
    def self.fetch
      new(ReadySet.raw_query('SHOW READYSET STATUS'))
    end

    # Creates a new `Status` based on the rows returned by invoking `SHOW READYSET STATUS`. These
    # rows have two columns, "name" and "value". We expect a well-defined set of possible "names"
    # and "values" that we parse into a `Status` object.
    #
    # @params [Array<Hash<String, String>] rows the rows returned by invoking
    # `SHOW READYSET STATUS` against ReadySet
    # @return [ReadySet::Status] an object representing ReadySet's current status
    def initialize(rows)
      attributes = rows.each_with_object({}) { |row, acc| acc[row['name']] = row['value'] }

      @connection_count = attributes['Connection Count'].to_i
      @connected_to_database = attributes['Database Connection'] == 'Connected'
      @last_completed_snapshot = parse_timestamp_if_not_nil(attributes['Last completed snapshot'])
      @last_replicator_error = attributes['Last replicator error']
      @last_started_controller = parse_timestamp_if_not_nil(attributes['Last started Controller'])
      @last_started_replication = parse_timestamp_if_not_nil(attributes['Last started replication'])
      @minimum_replication_offset = attributes['Minimum Replication Offset']
      @maximum_replication_offset = attributes['Maximum Replication Offset']
      @controller_status = attributes['ReadySet Controller Status']
      @snapshotting_completed = attributes['Snapshot Status'] == 'Completed'
    end

    # Returns true if ReadySet has successfully estblished a connection to the database and false
    # otherwise.
    #
    # @return [Boolean]
    alias_attribute :connected_to_database?, :connected_to_database

    # Reloads ReadySet's latest status and mutates the receiver of this method with the updated
    # information.
    #
    # @return [void]
    def reload
      Status.fetch.tap do |reloaded|
        @connection_count = reloaded.connection_count
        @connected_to_database = reloaded.connected_to_database
        @last_completed_snapshot = reloaded.last_completed_snapshot
        @last_replicator_error = reloaded.last_replicator_error
        @last_started_controller = reloaded.last_started_controller
        @last_started_replication = reloaded.last_started_replication
        @minimum_replication_offset = reloaded.minimum_replication_offset
        @maximum_replication_offset = reloaded.maximum_replication_offset
        @controller_status = reloaded.controller_status
        @snapshotting_completed = reloaded.snapshotting_completed
      end
    end

    # Returns true if ReadySet has finished snapshotting all of the tables from the database and
    # false otherwise.
    #
    # @return [Boolean]
    alias_attribute :snapshotting_completed?, :snapshotting_completed

    protected

    attr_reader :connected_to_database, :snapshotting_completed

    private

    def parse_timestamp_if_not_nil(timestamp)
      unless timestamp.nil?
        Time.parse(timestamp)
      end
    end
  end
end
