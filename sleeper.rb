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
  in_processes: 5,
  progress: "#{count}",
  finish: Proc.new{ |_,index,result| $stats[index] = result },
  max_rate: 20,
  begin: Proc.new{ $start_time = Time.now },
  end: Proc.new{ $end_time = Time.now },
}

setting = YAML.load(File.read(DISTRIBUTE_CONFIG))
setting.each do |key,val|
  parallel_options[key.to_sym] = val
end


begin
  Parallel.each(1..count, parallel_options) do |n|
    realtime = Benchmark.realtime { 
      now = Time.now.strftime("%T")
      STDERR.write "#{now} worker ##$$ running\n"
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

elapsed = $end_time - $start_time

avg = $stats.map{ |worker,diff| diff}.reduce(:+) / $stats.size
printf "total: %4d calls, %6.2f call/seconds\n", $stats.size, $stats.size/elapsed

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
  printf "[%#{index_width}d] worker #{worker}: %4d calls, avg: %.6f sec\n", index, count, avg
end
