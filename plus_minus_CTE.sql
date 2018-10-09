----------------------------------------------------------------------------
--NBA Analytics Question 1: 
---SELF IMPOSED Restrictions: 
      --1.) Do not use an Imperative Programming language (ie. Python, C++, JS)
      --2.) In SQL, chain together all the work into one chain of CTEs, without breaking or lagging. 
---DATA irregularities: 
      --1.) Please ignore the team_id on the subsitution_event, they often change implying that on different games, same players were on different teams
      --2.) Because of that, trades must have occured in the dataset, to offset that, we need to create roster_map which is a schema createed in a
      ------CTE below that shows the complete roster player_id by team_id AND game_id
---Step by Step description::
-----1.) We need to curate a table that has all the timestamp of when a player was on court: 
----------Grainularity of table == Game_id x player_id x period x "time_block_on_court" (which includes an entrance & exit timestamp))
-----2.) For that to work, we need to query "player_sub_in" actions and "player_sub_out" actions from play_by_play table
----------and add quarter_start && quarter_end artificially to each player as if everyone who starts a quarter and ends one had  
----------a sub_in action or a sub_out action  [in the code, this is the result of CTE full_sub_action]
-----3.) Now that we have all the times per gameXquarterXplayer, we need to create a artifical dimension "marker" to achieve the correct 
----------grouping, that way when we SELECT first_value() and last_value() we get the times that a player enter && exited the game in all quarters
-----4.) Having created "time_in_game", the CTE that has been what we wanted to create, we JOIN it to roster_map to ensure the right player <-> team JOIN
-----5.) With that done, we can finally calculate the box-score +/- using simple math
-----6.) CTE "plus" gives us the sum of all points score while a player is on the court at a Player x Game x Team level
-----7.) CTE 'minus' does the same, except on the JOIN, it is when the team_id != your.team_id
-----8.) Lastly, to produce the desired 3 cols demanded in the answers, you SUM( plus & minus) and group by game_id X player_id
----------------------------------------------------------------------------

with phase_1_sub as (
select team_id
     , game_id
     , period::bigint
     , person_id as person_id
     , 'sub_in' as sub_action
     , 7200 as pc_time
     , 'quart_starts'
  from gl_data
union all
select team_id, game_id, period, person_2 as person_id, 'sub_in'as sub_action, pc_time, 'sub_in'
    from play_by_play
where 1=1
      and event_msg_type = 8
union all
select team_id, game_id, period, person_1 as person_id, 'sub_out'as sub_action, pc_time, 'sub_out'
    from play_by_play
where 1=1
      and event_msg_type = 8
      )
  ,end_qrt_sub as (
    select a.team_id
          ,a.game_id
          ,a.period
          ,a.person_id
          ,'sub_out' as sub_action
          , 0 as pc_time
          , 'quarter_end'
    from phase_1_sub as a
    left join phase_1_sub as b on b.game_id = a.game_id and b.period = a.period
          and b.person_id = a.person_id and b.pc_time < a.pc_time and b.sub_action = 'sub_out'
    where 1=1
          and a.sub_action = 'sub_in'
          and b.sub_action is null
  )
  , full_sub_action as (
  select * from phase_1_sub
  union all
  select * from end_qrt_sub
  )
  , jerry_jones as (
  select *
       , row_number() over(partition by game_id,person_id order by period, pc_time desc) as marker
  from full_sub_action
  where sub_action = 'sub_in'
union all
  select *
       , row_number() over(partition by game_id,person_id order by period, pc_time desc) as marker
  from full_sub_action
  where sub_action = 'sub_out'
  )
  , entrance_exit_time as (
  select game_id
        ,person_id
        ,period
        ,marker
        ,first_value(pc_time) over(partition by game_id, person_id, period, marker order by pc_time desc rows between unbounded preceding and unbounded following ) as entrance_time
        ,last_value(pc_time) over(partition by game_id, person_id, period, marker order by pc_time desc rows between unbounded preceding and unbounded following ) as exit_time
  from jerry_jones
  )
, time_in_game as (
  select game_id
       , person_id
       , period
       , entrance_time
       , exit_time
    from entrance_exit_time
    group by 1,2,3,4,5
  --order by person_id, period, entrance_time desc
  )
, roster_map as (
  select team_id, person_id, game_id
    from gl_data
  group by 1,2,3
  union distinct
  select gl.team_id, pbp.person_2, gl.game_id
    from gl_data gl
  JOIN play_by_play pbp on pbp.person_1 = gl.person_id and pbp.event_msg_type = 8 and pbp.game_id = gl.game_id
  group by 1,2,3
  )
, plus as (
  select tig.game_id
        ,tig.person_id
        ,tm.team_id
        ,sum(case when pos.event_msg_type = 1 then option_1 when pos.event_msg_type = 3 and option_1 = 1 then 1 else 0 end) as plus
  from time_in_game tig
  join roster_map tm on tm.person_id = tig.person_id
  JOIN play_by_play pos on pos.game_id = tig.game_id
                        and pos.event_msg_type in (1, 3)
                        and pos.period = tig.period
                        and pos.pc_time between tig.exit_time and tig.entrance_time
                        and pos.team_id = tm.team_id
                        and pos.game_id = tm.game_id
  group by 1,2,3
)
, mlinus as (
  select tig.game_id
        ,tig.person_id
        ,tm.team_id
        ,sum(case when neg.event_msg_type = 1 then option_1 when neg.event_msg_type = 3 and option_1 = 1 then 1 else 0 end) as minus
  from time_in_game tig
  join roster_map tm on tm.person_id = tig.person_id
  JOIN play_by_play neg on  neg.game_id = tig.game_id
                        and neg.event_msg_type in (1, 3)
                        and neg.period = tig.period
                        and neg.pc_time between tig.exit_time and tig.entrance_time
                        and neg.game_id = tm.game_id
                        and neg.team_id != tm.team_id
  group by 1,2,3
  )
  select p.game_id
       , p.person_id
       , sum(p.plus) - sum(m.minus) as plus_minus
    from plus p
    join mlinus m on m.game_id = p.game_id and m.person_id = p.person_id
  group by 1,2
  ;



