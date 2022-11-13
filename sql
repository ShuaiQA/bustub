1.select distinct(language) from akas order by language limit 10;

## 其中||'(mins)'没有想出来
2.select primary_title,premiered,runtime_minutes||' (mins)' from titles where genres like '%Sci-Fi%' order by runtime_minutes desc limit 10;

3.select * from (select name, died-born as age from people where born >=1900 union select name, 2022-born as age from people where died is null and born >= 1900) order by age desc limit 20

4.select name,cnt from people, (select person_id,count(*) as cnt from crew group by person_id order by cnt desc limit 20) as cnt where people.person_id=cnt.person_id; 

5.select t.ten||'0s' as ten,round(avg(rating),2) as avg,max(rating),min(rating), count(*)from ratings as r, (select premiered/10 as ten, title_id from titles where premiered is not null) as t where r.title_id = t.title_id group by t.ten order by avg desc, ten; 

#先是查找扮演电影的人里面有Cruise的电影，找到电影的titie_id as tt   [title_id] 
#然后找到title的发布是在1962,还有找到相应的vote,以及对应的title     [title_id| title| votes ]
#最后合成最后的情况 [title|votes]
6.select title,votes from
(select title_id from crew as c,
(select * from people where name like '%Cruise%') as p where c.person_id=p.person_id) as tt,
(select bb.title_id ,title,votes from 
(select votes,t.title_id from
(select title_id from titles where premiered = 1962) as t, ratings as r 
where r.title_id = t.title_id) as bb , 
akas where akas.title_id = bb.title_id) as uu 
where tt.title_id = uu.title_id 
order by votes desc 
limit 10;

# 找到'Army of Thieves' 上映的premiered 
# 找到所有的title_id 删除重复的，count
7.select count(distinct(title_id)) from titles where premiered in (select premiered from titles where title_id in (select distinct(title_id) from akas where title='Army of Thieves'));


8.select distinct(name) from people where person_id in( select person_id from crew where title_id in( select title_id from crew where person_id in ( select person_id from people where name='Nicole Kidman'))) order by name;










