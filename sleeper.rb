#!/usr/bin/env ruby
require 'bundler'
Bundler.require

require 'benchmark'
require 'parallel'
require 'yaml'

DISTRIBUTE_CONFIG = 'distribute.yml'

def die(msg)
  STDERR.write "FATAL: #{msg}\n"
  exit 1
end


count = (ARGV[0] || 8).to_i

$stats = Array.new(count)

parallel_options = {
  in_processes: 100,
  progress: "#{count}",
  finish: Proc.new{ |_,index,result| $stats[index] = result },
}
setting = YAML.load(File.read(DISTRIBUTE_CONFIG))
setting.each do |key,val|
  parallel_options[key.to_sym] = val
end


begin
  Parallel.each(1..count, parallel_options) do |n|
    realtime = Benchmark.realtime { 
      File.open("log", "a") do |f|
        f.write "#{Time.now.strftime('%Y/%m/%d %T')}\t#{$$}\t#{n.inspect}\n"
      end
      sleep 0.1
    }
    [ $$, realtime ]
  end
rescue Interrupt
  die "interrupted"
rescue Parallel::RemoteWorkerTimeout
  die "remote worker timeout"
rescue Errno::EMFILE
  die "EMFILE: too many file, check 'ulimit -n'"
rescue Errno::ECONNRESET
  die "ECONNRESET: lost server connetion"
end

avg = $stats.map{ |worker,diff| diff}.reduce(:+) / $stats.size
printf "total: %4d calls, %6.2f call/seconds\n", $stats.size, 1/avg

worker_stats = {}
$stats.each do |worker,diff|
  worker_stats[worker] ||= []
  worker_stats[worker].push diff
end

index_width = worker_stats.length.to_s.length
worker_stats.each_with_index do |(worker,diffs),index|
  count = diffs.size
  sum = diffs.reduce(:+)
  avg = sum / count
  printf "[%#{index_width}d] worker #{worker}: %4d calls, %6.2f call/seconds\n", index, count, 1/avg
end
