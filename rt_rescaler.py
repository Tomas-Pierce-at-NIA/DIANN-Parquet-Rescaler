
import polars as pl
import argparse
import sys

parser = argparse.ArgumentParser(description="""Tool for linear transform of retention time in parquet file. 
                                                Allows viewing of top n rows for convenience.
                                                """)
parser.add_argument('--infile', required=True, help="Use specified parquet file")
parser.add_argument('--outfile', default=argparse.SUPPRESS, help='Output transformed file to OUTFILE')
parser.add_argument('--slope', type=float, default=1, help="Slope to multiple retention time column by")
parser.add_argument('--intercept', type=float, default=0, help='Intercept to add to retention time column after slope applied')
parser.add_argument('--col', default='RT', help='Column to linear transform, default RT')
parser.add_argument('--template', nargs=1, default=argparse.SUPPRESS, help="Template file to transform input file to")
parser.add_argument('--head', nargs='?', default=argparse.SUPPRESS, const=20, type=int, help="Display HEAD first rows of input file and exit, default 20")
namespace = parser.parse_args()

intable = pl.read_parquet(namespace.infile)

if 'head' in namespace:
    nrow = namespace.head
    head = intable.head(nrow)
    #print(head)
    head.glimpse()
    exit(0)
else:
    colname = namespace.col
    if 'template' not in namespace:
        slope = namespace.slope
        intercept = namespace.intercept
        outtable = intable.with_columns(pl.col(colname).mul(slope).add(intercept))
        if 'outfile' in namespace:
            outfile = namespace.outfile
            outtable.write_parquet(outfile)
        else:
            outtable.glimpse()
    else:
        template_table = pl.read_parquet(namespace.template)
        

