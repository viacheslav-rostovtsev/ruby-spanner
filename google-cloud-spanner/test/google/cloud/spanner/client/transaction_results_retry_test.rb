# Copyright 2017 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "helper"

describe Google::Cloud::Spanner::Client, :read, :error, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:tx_selector) do
      Google::Cloud::Spanner::V1::TransactionSelector.new(
        begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
          read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(
            read_lock_mode: :READ_LOCK_MODE_UNSPECIFIED
          )
        )
      )
  end

  let (:tx_1_id) {"$abc123"}
  let(:tx_1) do
    {
      id: tx_1_id,
    }
  end

  let(:tx_2) do
    {
      id: "$def456",
    }
  end

  let :results_hash1_tx1 do
    {
      metadata: {
        row_type: {
          fields: [
            { name: "id",          type: { code: :INT64 } },
            { name: "name",        type: { code: :STRING } },
            { name: "active",      type: { code: :BOOL } },
            { name: "age",         type: { code: :INT64 } },
            { name: "score",       type: { code: :FLOAT64 } },
            { name: "updated_at",  type: { code: :TIMESTAMP } },
            { name: "birthday",    type: { code: :DATE} },
            { name: "avatar",      type: { code: :BYTES } },
            { name: "project_ids", type: { code: :ARRAY,
                                           array_element_type: { code: :INT64 } } }
          ]
        },
        transaction: tx_1
      },
      precommit_token: {
        precommit_token: "token_1".force_encoding(Encoding::BINARY),
        seq_num: 0,
      }
    }
  end

  let :results_hash1_tx2 do
    {
      metadata: {
        row_type: {
          fields: [
            { name: "id",          type: { code: :INT64 } },
            { name: "name",        type: { code: :STRING } },
            { name: "active",      type: { code: :BOOL } },
            { name: "age",         type: { code: :INT64 } },
            { name: "score",       type: { code: :FLOAT64 } },
            { name: "updated_at",  type: { code: :TIMESTAMP } },
            { name: "birthday",    type: { code: :DATE} },
            { name: "avatar",      type: { code: :BYTES } },
            { name: "project_ids", type: { code: :ARRAY,
                                           array_element_type: { code: :INT64 } } }
          ]
        },
        transaction: tx_2
      },
      precommit_token: {
        precommit_token: "token_2".force_encoding(Encoding::BINARY),
        seq_num: 0,
      }
    }
  end
  let :results_hash2 do
    {
      values: [
        { string_value: "1" },
        { string_value: "Charlie" }
      ],
    }
  end
  let :results_hash3 do
    {
      values: [
        { bool_value: true},
        { string_value: "29" }
      ]
    }
  end
  let :results_hash4 do
    {
      values: [
        { number_value: 0.9 },
        { string_value: "2017-01-02T03:04:05.060000000Z" }
      ],
    }
  end
  let :results_hash5 do
    {
      values: [
        { string_value: "1950-01-01" },
        { string_value: "aW1hZ2U=" },
      ]
    }
  end
  let :results_hash6 do
    {
      values: [
        { list_value: { values: [ { string_value: "1"},
                                 { string_value: "2"},
                                 { string_value: "3"} ]}}
      ]
    }
  end
  let(:results_enum_tx_1) do
    [
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash1_tx1),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash2),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash3),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash4),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash5),
      GRPC::Unavailable,
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash6)
    ].to_enum
  end

  let(:results_enum_tx_2) do
    [
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash1_tx2),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash2),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash3),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash4),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash5),
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash6)
    ].to_enum
  end

  let(:commit_resp) do
    Google::Cloud::Spanner::V1::CommitResponse.new(
      commit_timestamp: Google::Cloud::Spanner::Convert.time_to_timestamp(Time.now),
      commit_stats: Google::Cloud::Spanner::V1::CommitResponse::CommitStats.new(
        mutation_count: 5
      )
    )
  end

  let(:client) { spanner.client instance_id, database_id }



  focus
  it "retries retriable errors" do
    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]

    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    
    streaming_read_request = [{
      session: session_grpc.name,
      table: "my-table",
      columns: ["id", "name", "active", "age", "score", "updated_at", "birthday", "avatar", "project_ids"],
      key_set: Google::Cloud::Spanner::V1::KeySet.new(all: true), 
      transaction: tx_selector,
      index: nil, limit: nil, resume_token: nil, partition_token: nil,
      request_options: nil,
      order_by: nil, lock_hint: nil
    }, default_options]

    commit_request = [{
       session: session_grpc.name, 
       transaction_id: tx_1_id,
       single_use_transaction: nil,
       mutations: [],
       request_options: nil 
    }, default_options]

    mock.expect :streaming_read, RaiseableEnumerator.new(results_enum_tx_1), streaming_read_request

    # this is very wrong, the request should not be identical, second request should have first transaction id
    mock.expect :streaming_read, RaiseableEnumerator.new(results_enum_tx_2), streaming_read_request

    mock.expect :commit, commit_resp, commit_request

    spanner.service.mocked_service = mock

    # @type [::Google::Cloud::Spanner::Client]
    sp_client = client
    
    sp_client.transaction do |tx|
      res = tx.read("my-table", columns)
      res.rows.to_a
      _(tx.transaction_id).must_equal tx_1_id
      _(res.transaction.id).must_equal tx_1_id
      _(res.instance_variable_get(:@current_precommit_token).precommit_token).must_equal "token_2"
    end

    shutdown_client! sp_client

    mock.verify
  end
end
