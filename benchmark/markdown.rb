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

def format_n_records(n_records)
  if n_records < 1_000_000
    "%dK" % (n_records / 1_000.0)
  else n_records
    "%dM" % (n_records / 1_000_000.0)
  end
end

benchmarks.each do |benchmark|
  base_dir = File.join(__dir__, benchmark)
  data = CSV.read(File.join(base_dir, "result.csv"),
                  headers: true,
                  converters: :all)
  readme_path = File.join(base_dir, "README.md")
  graph_markdown = "![Graph](result.svg)"
  readme = File.read(readme_path)
  readme_before_graph_markdown =
    readme.split(/^#{Regexp.escape(graph_markdown)}$/, 2)[0]
  records_per_n = data.each.group_by do |row|
    row["N records"]
  end
  result = ""
  records_per_n.each do |n_records, rows|
    result << "\n"
    result << "#{format_n_records(n_records)} records:\n"
    result << "\n"
    approaches = rows.collect do |row|
      approach = row["Approach"]
      if approach == "Apache Arrow Flight SQL"
        approach
      else
        "`#{approach}`"
      end
    end
    result << ("| " + approaches.join(" | ") + " |\n")
    separators = approaches.collect {|approach| "-" * approach.size}
    result << ("| " + separators.join(" | ") + " |\n")
    formatted_elapsed = approaches.zip(rows).collect do |approach, row|
      width = approach.size
      ("%.3f" % row["Elapsed time (sec)"]).ljust(width)
    end
    result << ("| " + formatted_elapsed.join(" | ") + " |\n")
  end
  File.write(readme_path, <<-README)
#{readme_before_graph_markdown.strip}

#{graph_markdown}

#{result.strip}
  README
end
