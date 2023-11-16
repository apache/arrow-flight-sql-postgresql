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

require "csv"

benchmarks = [
  "integer",
  "string",
]

types = [
  "select",
  "insert",
]

def format_n_records(n_records)
  if n_records < 1_000_000
    "%dK" % (n_records / 1_000.0)
  else n_records
    "%dM" % (n_records / 1_000_000.0)
  end
end

benchmarks.each do |benchmark|
  base_dir = File.join(__dir__, benchmark)
  readme_path = File.join(base_dir, "README.md")
  results_header = "## Results"
  readme = File.read(readme_path)
  readme_before_results_header =
    readme.split(/^#{Regexp.escape(results_header)}$/, 2)[0]
  results = ""
  types.each do |type|
    results << "### `#{type.upcase}`\n"
    results << "\n"
    results << "![Graph](#{type}.svg)"
    results << "\n"
    data = CSV.read(File.join(base_dir, "#{type}.csv"),
                    headers: true,
                    converters: :all)
    records_per_n = data.each.group_by do |row|
      row["N records"]
    end
    records_per_n.each do |n_records, rows|
      results << "\n"
      results << "#{format_n_records(n_records)} records:\n"
      results << "\n"
      approaches = rows.collect do |row|
        approach = row["Approach"]
        if approach == "Apache Arrow Flight SQL"
          approach
        else
          "`#{approach}`"
        end
      end
      results << ("| " + approaches.join(" | ") + " |\n")
      separators = approaches.collect {|approach| "-" * approach.size}
      results << ("| " + separators.join(" | ") + " |\n")
      formatted_elapsed = approaches.zip(rows).collect do |approach, row|
        width = approach.size
        ("%.3f" % row["Elapsed time (sec)"]).ljust(width)
      end
      results << ("| " + formatted_elapsed.join(" | ") + " |\n")
    end
    results << "\n"
  end
  File.write(readme_path, <<-README)
#{readme_before_results_header.strip}

#{results_header}

#{results.strip}
  README
end
