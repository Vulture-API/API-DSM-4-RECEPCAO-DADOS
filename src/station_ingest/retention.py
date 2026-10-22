"""SCRUM-530: trim only below age AND every group's safe boundary (Redis 7)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

SAFE_TRIM = r"""
local function parts(id)
  if type(id)~='string' then return nil end
  local a,b=string.match(id,'^(%d+)%-(%d+)$')
  if not a then return nil end
  a=string.gsub(a,'^0+',''); b=string.gsub(b,'^0+','')
  if a=='' then a='0' end; if b=='' then b='0' end
  return {a,b}
end
local function less(a,b)
  local x,y=parts(a),parts(b)
  for i=1,2 do
    if #x[i]~=#y[i] then return #x[i]<#y[i] end
    if x[i]~=y[i] then return x[i]<y[i] end
  end
  return false
end
if redis.call('EXISTS',KEYS[1])==0 then return {0,'','missing_stream',''} end
local groups=redis.call('XINFO','GROUPS',KEYS[1])
if #groups==0 then return {0,'','no_groups',''} end
if #groups>tonumber(ARGV[4]) then return {0,'','too_many_groups',''} end
local required=cjson.decode(ARGV[2])
local found={}
for _,g in ipairs(groups) do
  for i=1,#g,2 do if g[i]=='name' then found[g[i+1]]=true end end
end
for _,name in ipairs(required) do if not found[name] then return {0,'','required_group_missing',name} end end
local now=redis.call('TIME')
local ms=math.max(0,tonumber(now[1])*1000+math.floor(tonumber(now[2])/1000)-tonumber(ARGV[1])*1000)
local boundary=string.format('%.0f',ms)..'-0'
local reason,blocker='age',''
for _,g in ipairs(groups) do
  local info={}
  for i=1,#g,2 do info[g[i]]=g[i+1] end
  local delivered=info['last-delivered-id']
  if not parts(delivered) then return {0,'','invalid_metadata',info['name'] or ''} end
  if less(delivered,boundary) then boundary=delivered; reason='group_progress'; blocker=info['name'] end
  local pending=redis.call('XPENDING',KEYS[1],info['name'])
  if pending[1]>0 then
    if not parts(pending[2]) then return {0,'','invalid_metadata',info['name']} end
    if less(pending[2],boundary) then boundary=pending[2]; reason='pending'; blocker=info['name'] end
  end
end
-- The only mutation is last. Approximation may retain EXTRA entries, never
-- remove an entry at/above this boundary. No MAXLEN or age-only fallback.
local removed=redis.call('XTRIM',KEYS[1],'MINID','~',boundary,'LIMIT',ARGV[3])
return {removed,boundary,reason,blocker}
"""


@dataclass(frozen=True, slots=True)
class RetentionResult:
    removed: int
    boundary: str
    reason: str
    blocking_group: str


async def enforce_once(
    redis: Any,
    stream: str,
    retention_seconds: int,
    *,
    required_groups: list[str] | None = None,
    trim_limit: int = 10000,
    max_groups: int = 128,
) -> RetentionResult:
    groups = ["postgres-persister"] if required_groups is None else required_groups
    if not groups or retention_seconds < 1 or trim_limit < 1 or max_groups < 1:
        raise ValueError("Retention requires groups and positive limits")
    result = await redis.eval(
        SAFE_TRIM,
        1,
        stream,
        retention_seconds,
        json.dumps(groups),
        trim_limit,
        max_groups,
    )
    return RetentionResult(int(result[0]), result[1], result[2], result[3])
