####
# This is an example of slightly faster range joins using equi_join.
# This represents about a 2x boost over the older cross_join method at
#   https://github.com/Paradigm4/variant_warehouse/blob/master/rmarkdown/vcf_toolkit.R
# 
# For a new, much faster large-to-small Streaming example, see:
#   https://github.com/Paradigm4/streaming/blob/master/r_pkg/vignettes/ranges.Rmd
#
# I want to keep this code around because it's useful for the large-to-large case with small ranges. FWIW.

library('scidb')
scidbconnect()
KG_VARIANT    =scidb("KG_VARIANT")
KG_CHROMOSOME =scidb("KG_CHROMOSOME")
GENE          =scidb("GENE_37")

equi_range_join = function()
{
  top_n=200
  left = transform(KG_VARIANT, length= end - start + 1)
  right = transform(index_lookup(GENE, KG_CHROMOSOME, attr="chromosome", new_attr="chromosome_id"), length= end-start+1)
  left_stats  =  aggregate(transform(left, length="end - start + 1"), FUN="max(length) as length, count(*) as count")[]
  right_stats =  aggregate(transform(right,length="end - start + 1"), FUN="max(length) as length, count(*) as count")[]
  bucket_size = max(left_stats$length, right_stats$length)+1
  filter_condition = "LEFT.start <= RIGHT.end and RIGHT.start <= LEFT.end"
  left = project(left, "qual")
  right = project(right, c("chromosome_id", "start", "end", "gene"))
  res = scidb(sprintf("grouped_aggregate(
                      equi_join(
                      apply(
                      cross_join( 
                      %s,
                      build(<f:bool>[offset=0:1,2,0], true)
                      ),
                      bucket, iif(offset = 0, start / %i, iif(end / %i <> start / %i, end / %i, null)),
                      start, start,
                      end, end
                      ) as LEFT,
                      apply(
                      cross_join( 
                      %s,
                      build(<f:bool>[offset=0:1,2,0], true)
                      ),
                      bucket, iif(offset = 0, start / %i, iif(end / %i <> start / %i, end / %i, null))
                      ) as RIGHT,
                      'left_names=chromosome_id, bucket',
                      'right_names=chromosome_id, bucket',
                      'filter: %s and (LEFT.start / %i = LEFT.end / %i or RIGHT.start / %i = RIGHT.end / %i or LEFT.start / %i = bucket)'
                      ), count(*) as num_variants, gene)",
                      left@name, bucket_size, bucket_size, bucket_size, bucket_size,
                      right@name, bucket_size, bucket_size, bucket_size, bucket_size,
                      filter_condition, bucket_size, bucket_size, bucket_size, bucket_size, bucket_size
                      ))
  res = sort(res, attributes="num_variants", decreasing=TRUE)
  res = subset(res, n<top_n)[]
  print(qplot(x=res$n, y=res$num_variants))
  head(res, n=10)
}
