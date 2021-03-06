%help;

while(<>) {
    if(/^([a-z0-9_-]+):.*\#\#(?:@(\w+))?\s(.*)$/) {
        push(@{$help{$2}}, [$1, $3]);
    }
};

print "usage: make [target] [VARIABLE=VALUE]\n\n";

print "parameters:\n";
printf("  %-20s %s\n", "BQ_DATASET", "Dataset ID to upload the data in BigQuery");
printf("  %-20s %s\n", "BQ_VIEWS", "Dataset ID to mirror the data in BigQuery");
printf("  %-20s %s\n", "GOOGLE_CLOUD_PROJECT", "Project name with the GCS bucket and BigQuery dataset");
printf("  %-20s %s\n", "GCS_BUCKET", "Bucket name in Cloud Storage");
print "\n";

for ( sort keys %help ) {
    print "$_:\n";
    printf("  %-20s %s\n", $_->[0], $_->[1]) for @{$help{$_}};
    print "\n";
}
