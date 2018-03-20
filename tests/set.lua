-- ======================================================================
-- unique_set_write( rec, setBinName, newValue )
-- ======================================================================
-- This UDF adds a new value to a unique list.  If the value is already
-- present in the set, a "unique violation" error is returned.
--
-- For simplicity, we basically turn all values into strings, unless the
-- value is already a string.  That simplifies compares.
--
-- Parms:
-- (*) rec: the main Aerospike Record
-- (*) setBinName
-- (*) newValue: the new value passed in by the user
-- Return:
-- (Success): 0
-- (Error): Map{status=0, msg="Error Message"}
-- ======================================================================
function unique_set_write( rec, udfBin, newValue )
  local meth = "unique_set_write()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(newValue), type(newValue));
  local rc = 0;
  local setCtrl;
  local setList;
  local stringValue = tostring( newValue ); -- everything is a string

  -- Check for the record and the bin, create if not yet there.
  if( not aerospike:exists(rec) ) then
    rc = aerospike:create( rec );
    if( rc == nil or rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    else
      info("<CHECK><%s> Check Create Result(%s)", meth, tostring(rc));
    end
  end
  if( rec[udfBin] == nil ) then
    trace("[DB]<%s> Creating new Bin(%s)", meth, udfBin );
    -- It's a new bin, so everything starts out fresh
    setCtrl = map();
    setList = list();
    list.append( setList, newValue );
    setCtrl.SetList = setList;
  else
    trace("[DB]<%s> Using Existing Bin(%s)", meth, udfBin );
    -- It's an existing record, get the list, check for uniqueness and then
    -- insert if it's not already there.
    setCtrl = rec[udfBin];
    setList = setCtrl.SetList;
    if setList == nil then
      setList = list();
      setCtrl.SetList = setList;
    end
    local listSize = list.size( setList );
    trace("[DB]<%s> Map(%s) Searching list(%s) for Val(%s)",
            meth, tostring(setCtrl), tostring(setList), stringValue );
    local isNewValue = 1
    for i = 1, listSize, 1 do
      if( stringValue == tostring( setList[i] ) ) then
        isNewValue = 0
        break
      end
    end
    if (isNewValue == 1) then
      list.append( setList, newValue );
    end
  end
  -- end else record already exists
  -- store the set structure back into the record (this is necessary).
  rec[udfBin] = setCtrl;

  -- All done -- update the record (checking for errors on update)
  rc = aerospike:update( rec );
  info("[DEBUG]<%s> record update: rc(%s)", meth,tostring(rc));

  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]<%s> record update: rc(%s)", meth,tostring(rc));
    error("ERROR updating the record");
  end
  info("<CHECK><%s> Check Update Result(%s)", meth, tostring(rc));
  rc = 0;
  trace("[EXIT]:<%s> RC(0)", meth );
  return 0;
end -- unique_set_write()


-- ======================================================================
-- unique_set_write_many( rec, setBinName, values )
-- ======================================================================
-- This UDF adds a new value to a unique list.  If the value is already
-- present in the set, a "unique violation" error is returned.
--
-- For simplicity, we basically turn all values into strings, unless the
-- value is already a string.  That simplifies compares.
--
-- Parms:
-- (*) rec: the main Aerospike Record
-- (*) setBinName
-- (*) values: the new value passed in by the user
-- Return:
-- (Success): 0
-- (Error): Map{status=0, msg="Error Message"}
-- ======================================================================
function unique_set_write_many( rec, udfBin, values )
  local meth = "unique_set_write_many()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(values), type(values));
  local rc = 0;
  local setCtrl;
  local setList;
  local valuesCount = list.size(values); -- everything is a string

  -- Check for the record and the bin, create if not yet there.
  if( not aerospike:exists(rec) ) then
    rc = aerospike:create( rec );
    if( rc == nil or rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    else
      info("<CHECK><%s> Check Create Result(%s)", meth, tostring(rc));
    end
  end
  if( rec[udfBin] == nil ) then
    trace("[DB]<%s> Creating new Bin(%s)", meth, udfBin );
    -- It's a new bin, so everything starts out fresh
    setCtrl = map();
    setList = list();
    for i = 1, valuesCount, 1 do
      local newValue = tostring(values[i]);
      list.append( setList, newValue );
    end
    setCtrl.SetList = setList;
  else
    trace("[DB]<%s> Using Existing Bin(%s)", meth, udfBin );
    -- It's an existing record, get the list, check for uniqueness and then
    -- insert if it's not already there.
    setCtrl = rec[udfBin];
    setList = setCtrl.SetList;
    if setList == nil then
      setList = list();
      setCtrl.SetList = setList;
    end
    local exists = map()
    local listSize = list.size(setList)
    for i = 1, listSize, 1 do
      exists[setList[i]] = 1
    end
    for i = 1, valuesCount, 1 do
      if exists[values[i]] == nil then
       list.append(setList, tostring(values[i]))
      end
    end
  end
  -- end else record already exists
  -- store the set structure back into the record (this is necessary).
  rec[udfBin] = setCtrl;

  -- All done -- update the record (checking for errors on update)
  rc = aerospike:update( rec );
  info("[DEBUG]<%s> record update: rc(%s)", meth,tostring(rc));

  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]<%s> record update: rc(%s)", meth,tostring(rc));
    error("ERROR updating the record");
  end
  info("<CHECK><%s> Check Update Result(%s)", meth, tostring(rc));
  rc = 0;
  trace("[EXIT]:<%s> RC(0)", meth );
  return 0;
end -- unique_set_write()



-- ======================================================================
-- unique_set_read( rec, setBinName, searchValue )
-- ======================================================================
-- This UDF searches the list and returns 1 if the value is found and 0
-- if not found.
--
-- For simplicity, we basically turn all values into strings, unless the
-- value is already a string.  That simplifies compares.
--
-- Parms:
-- (*) rec: the main Aerospike Record
-- (*) setBinName
-- (*) searchValue: the value we're looking for
-- Return:
-- (Found):1
-- (Not Found):0
-- ======================================================================
function unique_set_read( rec, udfBin, searchValue )
  local meth = "unique_set_read()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(searchValue), type(searchValue));
  local result = 0; -- start off pessimistic
  local stringValue = tostring(searchValue);

  -- Check for the record and the bin proceed only if found.
  if( aerospike:exists( rec ) and rec[udfBin] ~= nil ) then
    local setCtrl = rec[udfBin];
    local setList = setCtrl.SetList;
    local listSize = list.size( setList );
    for i = 1, listSize, 1 do
      if( stringValue == tostring( setList[i] ) ) then
        result = 1;
        break;
      end
    end
  end
  trace("[EXIT]:<%s> Result(%d)", meth, result);
  return result;
end -- unique_set_read()

-- ======================================================================
-- unique_set_scan( rec, setBinName )
-- ======================================================================
-- This UDF returns a list that contains all elements of the unique set.
--
-- Parms:
-- (*) rec: the main Aerospike Record
-- (*) setBinName
-- Return:
-- List, either empty or holding set contents
-- ======================================================================
function unique_set_scan( rec, udfBin )
  local meth = "unique_set_scan()";
  trace("[ENTER]<%s> udfBin(%s)", meth, tostring(udfBin));
  local resultList = list();

  -- Check for the record and the bin proceed only if found.
  if( aerospike:exists( rec ) and rec[udfBin] ~= nil ) then
    local setCtrl = rec[udfBin];
    local setList = setCtrl.SetList;
    if setList == nil then
      return list();
    end
    local listSize = list.size( setList );
    resultList = list.take( setList, listSize );
  end
  trace("[EXIT]:<%s> ResultList(%s)", meth, tostring(resultList));
  if resultList == nil then
    return list();
  end
  return resultList;
end -- unique_set_scan()


function unique_set_remove( rec, udfBin, newValue )
  local meth = "unique_set_remove()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(newValue), type(newValue));
  local rc = 0;
  local setCtrl;
  local setList;
  local stringValue = tostring( newValue ); -- everything is a string

  -- Check for the record and the bin, create if not yet there.
  if( not aerospike:exists(rec) ) then
    rc = aerospike:create( rec );
    if( rc == nil or rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    else
      info("<CHECK><%s> Check Create Result(%s)", meth, tostring(rc));
    end
  end
  if( rec[udfBin] == nil ) then
    trace("[DB]<%s> Creating new Bin(%s)", meth, udfBin );
    -- It's a new bin, so everything starts out fresh
    setCtrl = map();
    setList = list();
    setCtrl.SetList = setList;
  else
    trace("[DB]<%s> Using Existing Bin(%s)", meth, udfBin );
    -- It's an existing record, get the list, check for uniqueness and then
    -- insert if it's not already there.
    setCtrl = rec[udfBin];
    setList = setCtrl.SetList;
    local newList = list();
    if setList == nil then
      setList = list();
      setCtrl.SetList = setList;
    end
    local listSize = list.size( setList );
    trace("[DB]<%s> Map(%s) Searching list(%s) for Val(%s)",
            meth, tostring(setCtrl), tostring(setList), stringValue );
    for i = 1, listSize, 1 do
      if( stringValue ~= tostring( setList[i] ) ) then
        list.append(newList, setList[i])
      end
    end
    setCtrl.SetList = newList;
  end
  -- end else record already exists
  -- store the set structure back into the record (this is necessary).
  rec[udfBin] = setCtrl;

  -- All done -- update the record (checking for errors on update)
  rc = aerospike:update( rec );
  info("[DEBUG]<%s> record update: rc(%s)", meth,tostring(rc));

  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]<%s> record update: rc(%s)", meth,tostring(rc));
    error("ERROR updating the record");
  end
  info("<CHECK><%s> Check Update Result(%s)", meth, tostring(rc));
  rc = 0;
  trace("[EXIT]:<%s> RC(0)", meth );
  return 0;
end -- unique_set_write()


function unique_set_remove_many( rec, udfBin, values )
  local meth = "unique_set_write_many()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(values), type(values));
  local rc = 0;
  local setCtrl;
  local setList;
  local valuesCount = list.size(values); -- everything is a string

  -- Check for the record and the bin, create if not yet there.
  if( not aerospike:exists(rec) ) then
    rc = aerospike:create( rec );
    if( rc == nil or rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    else
      info("<CHECK><%s> Check Create Result(%s)", meth, tostring(rc));
    end
  end
  if( rec[udfBin] == nil ) then
    trace("[DB]<%s> Creating new Bin(%s)", meth, udfBin );
    -- It's a new bin, so everything starts out fresh
    setCtrl = map();
    setList = list();
    setCtrl.SetList = setList;
  else
    trace("[DB]<%s> Using Existing Bin(%s)", meth, udfBin );
    -- It's an existing record, get the list, check for uniqueness and then
    -- insert if it's not already there.
    setCtrl = rec[udfBin];
    setList = setCtrl.SetList;
    local newList = list();
    if setList == nil then
      setList = list();
      setCtrl.SetList = setList;
    end
    local listSize = list.size( setList );
    local exists = map()
    for i = 1, valuesCount, 1 do
      exists[tostring(values[i])] = 1
    end
    for i = 1, listSize, 1 do
      if exists[setList[i]] == nil then
       list.append(newList, setList[i])
      end
    end
    setCtrl.SetList = newList;
  end
  -- end else record already exists
  -- store the set structure back into the record (this is necessary).
  rec[udfBin] = setCtrl;

  -- All done -- update the record (checking for errors on update)
  rc = aerospike:update( rec );
  info("[DEBUG]<%s> record update: rc(%s)", meth,tostring(rc));

  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]<%s> record update: rc(%s)", meth,tostring(rc));
    error("ERROR updating the record");
  end
  info("<CHECK><%s> Check Update Result(%s)", meth, tostring(rc));
  rc = 0;
  trace("[EXIT]:<%s> RC(0)", meth );
  return 0;
end -- unique_set_write()


function map_increment( rec, udfBin, mapKey, increment)
  local meth = "map_increment()";
  trace("[ENTER]<%s> udfBin(%s) Value(%s) valType(%s)",
    meth, tostring(udfBin), tostring(mapKey), type(increment));
  local rc = 0;
  increment = tonumber(increment)
  local setCtrl;

  -- Check for the record and the bin, create if not yet there.
  if( not aerospike:exists(rec) ) then
    rc = aerospike:create( rec );
    if( rc == nil or rec == nil ) then
      warn("[ERROR]<%s> Problems creating record", meth );
      error("ERROR creating record");
    else
      info("<CHECK><%s> Check Create Result(%s)", meth, tostring(rc));
    end
  end
  if( rec[udfBin] == nil ) then
    trace("[DB]<%s> Creating new Bin(%s)", meth, udfBin );
    -- It's a new bin, so everything starts out fresh
    setCtrl = map();
    setCtrl[mapKey] = increment;
  else
    trace("[DB]<%s> Using Existing Bin(%s)", meth, udfBin );
    -- It's an existing record, get the list, check for uniqueness and then
    -- insert if it's not already there.
    setCtrl = rec[udfBin];
    local currentValue = setCtrl[mapKey]
    if currentValue == nil then
      setCtrl[mapKey] = increment
    else
      setCtrl[mapKey] = increment + currentValue
    end
  end
  -- end else record already exists
  -- store the set structure back into the record (this is necessary).
  rec[udfBin] = setCtrl;

  -- All done -- update the record (checking for errors on update)
  rc = aerospike:update( rec );
  info("[DEBUG]<%s> record update: rc(%s)", meth,tostring(rc));

  if( rc ~= nil and rc ~= 0 ) then
    warn("[ERROR]<%s> record update: rc(%s)", meth,tostring(rc));
    error("ERROR updating the record");
  end
  info("<CHECK><%s> Check Update Result(%s)", meth, tostring(rc));
  rc = 0;
  trace("[EXIT]:<%s> RC(0)", meth );
  return 0;
end -- unique_set_write()
