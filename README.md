# HBase
Contains: InsertAppData.java, AlterVersion.java, HelpfulReview.java, 
ReviewTextFilter.java, LimitRatingsFilter.java, ReviewTimeFilter.java, 
RatingFilteredAsin.java, NameFilter.java

-------------------
InsertAppData.java
-------------------
Input Data: Apps_for_Android_5.json 
(Reviews on Apps for andriod)
Output:
Inserted 752937 rows

***PUT AND GET OPERATIONS***
------------------
AlterVersion.java 
------------------
Terminal Command> alter 'AppData', {'NAME'=>'Review', 'VERSIONS'=>4}
 - The column family "Review" has 4 versions now
 - Run AlterVersion.java multiple times with different summary each time.
 - Four most recent summaries are printed at the output.

Output (prints latest 4 versions of summary):
Addicted to this app
Really cute
This is cool
Refreshing!!

***DATA ANALYTICS WITH HBASE QUERIES***
-------------------
HelpfulReview.java
-------------------
- Filtered data based on denominator (>=5) of the fraction of helpfulness

Output:
The review rating for the App ASIN: B0080JJLBW is 64.0% helpful
The review rating for the App ASIN: B006VFNV8G is 91.66667% helpful
so on...
Number of helpful reviews: 93654

----------------------
ReviewTextFilter.java
----------------------
- Displays data that satisfies both filter conditions
- filter1 for reviewText = "subscription"
- filter2 for overall <= 3.0

Output:
ASIN: B0063HB1HU || Overall: 2.0 || Review Text: need a paid subscription to view, read, the readers digest. it is okay, removed it since i did not know a subscription was needed.
ASIN: B00FE3GXUY || Overall: 3.0 || Review Text: Since I don't want a subscription, the site gives me the overall news and I have 3 articles I can read completely. I'm fine with this. I'd recommend this for those who want an overall look at what is going on.
so on...
Total filtered data: 532

------------------------
LimitRatingsFilter.java
------------------------
- Filtered data based on 'overall' ratings greater than or equal to 4.0
- Prints top rated products
- Limited to print first 4 rows of filtered data

Output:
Overall rating: 5.0 || ASIN: B004WGGQPQ || Summary: #1 drawing game!!
Overall rating: 5.0 || ASIN: B006C1ZSO4 || Summary: Good for all ages.
Overall rating: 5.0 || ASIN: B006YUVTK0 || Summary: My toddler loves it!
Overall rating: 5.0 || ASIN: B0080JJLBW || Summary: Son love it
Total filtered data: 4

----------------------
ReviewTimeFilter.java
----------------------
- Displays data that satisfies one of the filter conditions
- filter1 for reviewText = "fun" as sub-string
- filter2 for unixReviewTime = 1331424000

Output:
ASIN: B005UQ9FNC || UnixReviewTime: 1331424000 || Review Text: this game is super fun!! My 3 year old love's. to play it. it's easy and a great time waister.A must try game fun for all :))
ASIN: B00HV0FBEM || UnixReviewTime: 1392076800 || Review Text: This game is a lot of fun and keeps you interested. For a free game, I am beyond impressed. Great game!!
ASIN: B00HW1D6EW || UnixReviewTime: 1393891200 || Review Text: This app is the perfect combination- works great, is fun, free, and doesn't bombard you with ads. It's both stimulating and fun. Once I start playing I have trouble putting it down.
so on...
Total filtered data: 201336

------------------------
RatingFilteredAsin.java
------------------------
- Displays number of reviews for a specific App and average overall rating provided by the users

Output:
Total reviews on App with ASIN: B004AZH4C8 --> 86
Average overall rating for App with ASIN: B004AZH4C8 is 4.8

------------------------
NameFilter.java
------------------------
- Displays person with a given first name 'Eric' who usually use 'great' for reviews to rate Apps of overall rating greater than or equal to 4.0 in the year 2013.
- Data satisfies all the filter conditions
- filter1 with First name = "Eric" in reviewerName
- filter2 with year = "2103" in reviewTime
- filter3 with overall >= 4.0
- filter4 with summary having "great" as sub-string

Output:
App Reviewer Name: Eric Josef Review Time: 09 6, 2013 Overall rating: 5.0 Summary: This game is great
App Reviewer Name: Eric M Stuve Review Time: 07 12, 2013 Overall rating: 4.0 Summary: great game, but...e'
so on...
Total filtered data: 35
