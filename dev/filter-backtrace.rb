#!/usr/bin/env ruby
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARGF.each_line do |line|
  puts(line)
  case line
  when /\A\s*([A-Za-z\d\/_.-]+)\(\+(0x\h+)\)/
    path = $1
    address = $2
    IO.pipe do |input_read, input_write|
      IO.pipe do |output_read, output_write|
        pid = spawn("addr2line", "--exe=#{path}", "%#x" % address,
                    in: input_read,
                    out: output_write)
        input_read.close
        input_write.close
        output_write.close
        Process.waitpid(pid)
        puts(output_read.read)
      end
    end
  end
end
