# Indents '#' style comments following instructions in scripts to align on columns 17 and 29 (mimicking tab stops of 8)
# Replaces some hex numbers with decimal
#
# Example:
#  write 65 # Size
#  write 0x01s # Fetch
# is transformed to
#  write 65        # Size
#  write 1s        # Fetch
#
# Usage
# =====
# find src/main/scripts/org/reaktivity/specification/kafka/fetch.v5/ -name "*.rpt"| xargs -L 1 sed -i "" -f tools/format_scripts.sed
#

#  Temporary: replace [n] with [0x0n]
s/\[\([1234567890]\)]/[0x0\1]/

# Remove trailing spaces
s/ *$//

# Replace hex numbers
s/0x41$/65/
s/0x41           #$/65             #/
s/ 0x00$/ 0/
s/ 0x01$/ 1/
s/ 0x50$/ 80/
s/ 0x44$/ 68/

/ 0x0[0123456789]/s/0x0//
/0x0[0123456789]s/s/0x0//
/0x0[0123456789]L/s/0x0//

# Align comments
/^[^#]\{4\}[^#]* * #/s/  *#/ #/

s/^\([^#]\{3\} \)#/\1            #/
s/^\([^#]\{4\} \)#/\1           #/
s/^\([^#]\{5\} \)#/\1          #/
s/^\([^#]\{6\} \)#/\1         #/
s/^\([^#]\{7\} \)#/\1        #/
s/^\([^#]\{8\} \)#/\1       #/
s/^\([^#]\{9\} \)#/\1      #/
s/^\([^#]\{10\} \)#/\1     #/
s/^\([^#]\{11\} \)#/\1    #/
s/^\([^#]\{12\} \)#/\1   #/
s/^\([^#]\{13\} \)#/\1  #/
s/^\([^#]\{14\} \)#/\1 #/

s/^\([^#]\{16\} \)#/\1           #/
s/^\([^#]\{17\} \)#/\1          #/
s/^\([^#]\{18\} \)#/\1         #/
s/^\([^#]\{19\} \)#/\1        #/
s/^\([^#]\{20\} \)#/\1       #/
s/^\([^#]\{21\} \)#/\1      #/
s/^\([^#]\{22\} \)#/\1     #/
s/^\([^#]\{23\} \)#/\1    #/
s/^\([^#]\{24\} \)#/\1   #/
s/^\([^#]\{25\} \)#/\1  #/
s/^\([^#]\{26\} \)#/\1 #/


