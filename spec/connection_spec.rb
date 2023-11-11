# connection_spec.rb
# spec/readyset-rails/connection_spec.rb

require 'spec_helper'
require_relative './../lib/ready_set/connection.rb'
require_relative './../lib/ready_set.rb'

RSpec.describe ReadySet::Connection do
  let(:connection_double) { instance_double('ActiveRecord::ConnectionAdapters::AbstractAdapter') }

  before do
    allow(ActiveRecord::Base).
      to receive(:establish_connection).
      and_return(nil) # No need to return true now

    allow(ActiveRecord::Base).to receive(:connection).and_return(connection_double)
  end

  describe '.establish' do
    context 'when the database is ready' do
      before do
        # Simulate the response structure from the database for the status check
        completed_status_response = [{ 'name' => 'Snapshot Status', 'value' => 'Completed' }]
        # For the "database is ready" context
        allow(connection_double).
          to receive(:execute).
          with('SHOW READYSET STATUS;').
          and_return(completed_status_response)
      end

      it 'establishes a connection without raising an error' do
        expect { ReadySet::Connection.establish }.not_to raise_error
      end
    end

    context 'when the database is not ready' do
      before do
        # Simulate the response structure from the database for the status check
        incomplete_status_response = [{ 'name' => 'Snapshot Status', 'value' => 'In Progress' }]

        # For the "database is not ready" context
        allow(connection_double).
          to receive(:execute).
          with('SHOW READYSET STATUS;').
          and_return(incomplete_status_response)
      end

      it 'raises an error' do
        expected_error = ReadySet::Connection::NotReadyError
        expected_error_message = 'ReadySet database is not ready for service!'

        expect { ReadySet::Connection.establish }.
          to raise_error(expected_error, expected_error_message)
      end
    end
  end
end
