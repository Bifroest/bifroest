
(1..10).each do |n|
  task "run#{n}" do
    sh "mvn -DconfigDir=test/cluster-config/bifroest-#{n} exec:java"
  end
end
