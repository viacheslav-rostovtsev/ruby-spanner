# Copyright 2025 Google LLC
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

require "concurrent"
require "google/cloud/spanner/session"

module Google
  module Cloud
    module Spanner
      # Cache for multiplex `{Google::Cloud::Spanner::Session}` instances.
      # @private
      class MultiplexSessionCache
        SESSION_REFRESH_SEC = 7 * 24 * 3600
        private_constant :SESSION_REFRESH_SEC

        # Create a single-session "cache" for multiplex sessions.
        # @param client [::Google::Cloud::Spanner::Client] A `Spanner::Client` ref
        # @private
        def initialize client
          @client = client
          @mutex = Mutex.new
        end

        # Yields a current session to run requests on.
        # @yield session A session to run requests on
        # @yieldparam session [::Google::Cloud::Spanner::Session]
        # @private
        # @yieldreturn [::Object] The result of the operation.
        # @return [::Object] The value returned by the yielded block.
        def with_session
          ensure_session!
          yield @session
        end

        # Re-initializes the session in the session cache
        # @private
        # @return [::Boolean]
        def reset!
          @mutex.synchronize do
            @session = @client.create_new_session multiplexed: true
          end

          true
        end

        # Closes the pool. This is a NOP for Multiplex Session Cache since
        # multiplex sessions don't require cleanup.
        # @private
        # @return [::Boolean]
        def close
          true
        end

        private

        # Ensures that a single session exists and is current.
        # @private
        # @return [nil]
        def ensure_session!
          return unless @session.nil? || @session.created_since?(SESSION_REFRESH_SEC)

          @mutex.synchronize do
            return unless @session.nil? || @session.created_since?(SESSION_REFRESH_SEC)
            @session = @client.create_new_session multiplexed: true
          end

          nil
        end
      end
    end
  end
end
