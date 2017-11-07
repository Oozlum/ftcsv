local ftcsv = {
    _VERSION = 'ftcsv 1.1.3',
    _DESCRIPTION = 'CSV library for Lua',
    _URL         = 'https://github.com/FourierTransformer/ftcsv',
    _LICENSE     = [[
        The MIT License (MIT)

        Copyright (c) 2016 Shakil Thakur

        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
    ]]
}

-- lua 5.1 load compat
-- luajit specific speedups
-- luajit performs faster with iterating over string.byte,
-- whereas vanilla lua performs faster with string.find
local M = {}
if type(jit) == 'table' or _ENV then
  M.load = _G.load
  M.find_char = function(chunk, start_pos, a, b, c)
    a = a and string.byte(a)
    b = b and string.byte(b)
    c = c and string.byte(c)

    local end_pos = chunk:len()

    while start_pos <= end_pos do
      local x = string.byte(chunk, start_pos)
      if x == a or x == b or x == c then
        return start_pos
      end
      start_pos = start_pos + 1
    end
    return nil
  end
else
  M.load = loadstring
  M.find_char = function(chunk, start_pos, a, b, c)
    local pattern = string.format('[%s%s%s]', a or '', b or '', c or '')
    return chunk:find(pattern, start_pos)
  end
end

-- parse and return a field from the given chunk.  Chunk is not guaranteed to
-- contain a complete field, a field may span chunks.  Field is a state object
-- which should be preserved between calls if the field is not complete
local function parse_field(chunk, separator, field)
  local function find_closing_quote(chunk, start_pos)
    start_pos = start_pos or 1
    local quote_pos = M.find_char(chunk, start_pos, '"')
    if not quote_pos or chunk:sub(quote_pos + 1, quote_pos + 1) ~= '"' then
      return quote_pos
    end
    return find_closing_quote(chunk, quote_pos + 2)
  end

  local function create_field()
    return {
      content = '',
      complete = true,
      quoted = false,
      ended_with_separator = false
    }
  end

  -- if we've reached the end of the input and have an incomplete field, see if we
  -- can complete it.  If the input ended with a separator, create an empty field.
  -- Return a completed field or nil
  if not chunk or chunk == "" then
    if field then
      if field.complete and field.ended_with_separator then
        field = create_field()
        field.complete = false
      end
      if not field.complete and not field.quoted then
        field.complete = true
        return field
      end
    end
    return nil
  end

  if not field or field.complete then
    field = create_field()
  end

  -- if this is the start of a new field, check for an opening quote and remove it.
  if field.complete and not field.quoted and chunk:sub(1, 1) == '"' then
    field.quoted = true
    chunk = chunk:sub(2)
  end

  -- if the field is quoted, add to the field content everything up to the closing
  -- quote, otherwise add everything up to the next field/record separator.
  local break_pos
  if field.quoted then
    break_pos = find_closing_quote(chunk)
  else
    break_pos = M.find_char(chunk, 1, separator, '\r', '\n')
  end

  -- remove the content from the chunk and append it to the existing field content.
  -- if we found a break, the field is complete, otherwise it is incomplete.
  field.complete = (break_pos and true) or false
  field.content = field.content .. chunk:sub(1, break_pos and break_pos - 1)
  chunk = break_pos and chunk:sub(break_pos) or ''

  -- if the field is quoted and we've found the end quote, remove the quote from
  -- the remainder, escape any double quotes and mark it complete.
  if field.quoted and chunk:sub(1, 1) == '"' then
    field.content = field.content:gsub('""', '"')
    chunk = chunk:sub(2)
  end

  -- if the field ended with a separator, make a note of it.
  if chunk:sub(1, 1) == separator then
    field.ended_with_separator = true
  end

  return field, chunk
end

local row_handler
row_handler = {
  data = nil,
  field = nil,
  reader = nil,
  separator = nil,

  -- return and reset the row data
  complete = function(self)
    local d = self.data
    self.data = nil
    return d
  end,

  -- parse and insert a field into the row, creating the row when the first field is found.
  parse_row = function(self, chunk)
    self.field, chunk = parse_field(chunk, self.separator, self.field)

    if self.field and self.field.complete then
      if self.field.content then
        self.data = self.data or {}
        self.data[#self.data + 1] = self.field.content
      end
    end

    if chunk then
      local next_char = chunk:sub(1, 1)
      if next_char == '\r' then
        chunk = chunk:sub(2)
        next_char = chunk:sub(1, 1)
      end
      self.reader.put_back(chunk:sub(2))
      if next_char == '\n' then
        return self:complete()
      end
    else
      return self:complete()
    end
  end,

  -- the iterator function which processes chunks until a row is complete
  -- or the chunk stream ends.
  iter = function(row)
    local chunk
    for chunk in row.reader.strings do
      local row_data = row:parse_row(chunk)
      if row_data then
        return row_data, row
      end
    end
    -- try to finish an incomplete row.
    return row:parse_row(), row
  end,

  -- create and return an instance of the row_handler
  create = function(reader, separator)
    return {
      data = nil,
      field = nil,
      reader = reader,
      separator = separator,
      complete = row_handler.complete,
      parse_row = row_handler.parse_row,
    }
  end
}

-- create and return a row parser iterator function
local function row_parser(reader, separator)
  local row = row_handler.create(reader, separator)
  return row_handler.iter, row
end

-- runs the show!
function ftcsv.parse(input, delimiter, options)
    -- delimiter MUST be one character
    assert(#delimiter == 1 and type(delimiter) == "string", "the delimiter must be of string type and exactly one character")
    assert(type(input) == "string" or type(input) == 'function', "input must be string or a function")

    -- OPTIONS yo
    local header = true
    local rename
    local fieldsToKeep = nil
    local loadFromString = false
    local headerFunc
    if options then
        if options.headers ~= nil then
            assert(type(options.headers) == "boolean", "ftcsv only takes the boolean 'true' or 'false' for the optional parameter 'headers' (default 'true'). You passed in '" .. tostring(options.headers) .. "' of type '" .. type(options.headers) .. "'.")
            header = options.headers
        end
        if options.rename ~= nil then
            assert(type(options.rename) == "table", "ftcsv only takes in a key-value table for the optional parameter 'rename'. You passed in '" .. tostring(options.rename) .. "' of type '" .. type(options.rename) .. "'.")
            rename = options.rename
        end
        if options.fieldsToKeep ~= nil then
            assert(type(options.fieldsToKeep) == "table", "ftcsv only takes in a list (as a table) for the optional parameter 'fieldsToKeep'. You passed in '" .. tostring(options.fieldsToKeep) .. "' of type '" .. type(options.fieldsToKeep) .. "'.")
            local ofieldsToKeep = options.fieldsToKeep
            if ofieldsToKeep ~= nil then
                fieldsToKeep = {}
                for j = 1, #ofieldsToKeep do
                    fieldsToKeep[ofieldsToKeep[j]] = true
                end
            end
            if header == false and options.rename == nil then
                error("ftcsv: fieldsToKeep only works with header-less files when using the 'rename' functionality")
            end
        end
        if options.loadFromString ~= nil then
            assert(type(options.loadFromString) == "boolean", "ftcsv only takes a boolean value for optional parameter 'loadFromString'. You passed in '" .. tostring(options.loadFromString) .. "' of type '" .. type(options.loadFromString) .. "'.")
            loadFromString = options.loadFromString
        end
        if loadFromString then
            assert(type(input) == "string", "optional parameter 'loadFromString' can only be used with a string input")
        end
        if options.headerFunc ~= nil then
            assert(type(options.headerFunc) == "function", "ftcsv only takes a function value for optional parameter 'headerFunc'. You passed in '" .. tostring(options.headerFunc) .. "' of type '" .. type(options.headerFunc) .. "'.")
            headerFunc = options.headerFunc
        end
    end

    -- generate iterator functions for the string input types.
    if type(input) == "string" then
      local inputString = input
      if not loadFromString then
        local file = io.open(input, "r")
        if not file then error("ftcsv: File not found at " .. input) end
        inputString = file:read("*all")
        file:close()
      end

      input = function()
        local s = inputString
        inputString = nil
        return s
      end
    end

    -- generate a reader function to wrap the input iterator function
    local function reader(iter)
      local remainder

      return {
        strings = function()
          local chunk = remainder
          remainder = nil
          if not chunk then
            chunk = iter()
          end
          return chunk
        end,
        put_back = function(chunk)
          if chunk ~= '' then
            remainder = chunk
          end
        end
      }
    end
    input = reader(input)

    local row_data
    local parser, row = row_parser(input, delimiter)

    -- parse the first row and generate the headers.
    row_data, row = parser(row)
    if not row_data then
      error('ftcsv: Cannot parse an empty file')
    elseif #row_data == 0 then
      error('ftcsv: Cannot parse a file which contains empty headers')
    end

    -- rename and transform the headers, storing both a forward and reverse index
    local headerIndex = {}
    local reverseHeaderIndex = {}
    for i,v in ipairs(row_data) do
      if header then
        headerIndex[i] = (rename and rename[v]) or v
      else
        headerIndex[i] = (rename and rename[i]) or i
      end
      if headerFunc then
        headerIndex[i] = headerFunc(headerIndex[i]) or headerIndex[i]
      end
      if headerIndex[i] == '' then
        error('ftcsv: Cannot parse a file which contains empty headers')
      end
      reverseHeaderIndex[headerIndex[i]] = i
    end

    -- generate the final, ordered header list, discarding any unwanted fields and
    -- removing duplicates keeping the last occurance
    local headerField = {}
    for i,v in ipairs(headerIndex) do
      if (not fieldsToKeep or fieldsToKeep[v]) and reverseHeaderIndex[v] == i then
        headerField[#headerField + 1] = v
      else
        headerIndex[i] = false
      end
    end

    -- done with the reverse index now.
    reverseHeaderIndex = nil

    -- create a new row in the output with the given data, skipping any unwanted
    -- columns
    local output = {}
    local function store_row(row_data)
      if not row_data then return end

      if #row_data < #headerIndex then
        error('ftcsv: too few columns in row '..tostring(#output + 1))
      elseif #row_data > #headerIndex then
        error('ftcsv: too many columns in row '..tostring(#output + 1))
      end

      local stored_data = {}
      for i,v in ipairs(row_data) do
        if headerIndex[i] then
          stored_data[headerIndex[i]] = v
        end
      end
      output[#output + 1] = stored_data
    end

    -- if the first row is not a header row, add it to the output
    if not header then
      store_row(row_data)
    end

    -- finally, iterate the remaining rows, adding them to the output
    for row_data, row in parser, row do
      store_row(row_data)
    end

    return output, headerField
end

-- a function that delimits " to "", used by the writer
local function delimitField(field)
    field = tostring(field)
    if field:find('"') then
        return field:gsub('"', '""')
    else
        return field
    end
end

-- a function that compiles some lua code to quickly print out the csv
local function writer(inputTable, dilimeter, headers)
    -- they get re-created here if they need to be escaped so lua understands it based on how
    -- they came in
    for i = 1, #headers do
        if inputTable[1][headers[i]] == nil then
            error("ftcsv: the field '" .. headers[i] .. "' doesn't exist in the inputTable")
        end
        if headers[i]:find('"') then
            headers[i] = headers[i]:gsub('"', '\\"')
        end
    end

    local outputFunc = [[
        local state, i = ...
        local d = state.delimitField
        i = i + 1;
        if i > state.tableSize then return nil end;
        return i, '"' .. d(state.t[i]["]] .. table.concat(headers, [["]) .. '"]] .. dilimeter .. [["' .. d(state.t[i]["]]) .. [["]) .. '"\r\n']]

    -- print(outputFunc)

    local state = {}
    state.t = inputTable
    state.tableSize = #inputTable
    state.delimitField = delimitField

    return M.load(outputFunc), state, 0

end

-- takes the values from the headers in the first row of the input table
local function extractHeaders(inputTable)
    local headers = {}
    for key, _ in pairs(inputTable[1]) do
        headers[#headers+1] = key
    end

    -- lets make the headers alphabetical
    table.sort(headers)

    return headers
end

-- turns a lua table into a csv
-- works really quickly with luajit-2.1, because table.concat life
function ftcsv.encode(inputTable, delimiter, options)
    local output = {}

    -- dilimeter MUST be one character
    assert(#delimiter == 1 and type(delimiter) == "string", "the delimiter must be of string type and exactly one character")

    -- grab the headers from the options if they are there
    local headers = nil
    if options then
        if options.fieldsToKeep ~= nil then
            assert(type(options.fieldsToKeep) == "table", "ftcsv only takes in a list (as a table) for the optional parameter 'fieldsToKeep'. You passed in '" .. tostring(options.headers) .. "' of type '" .. type(options.headers) .. "'.")
            headers = options.fieldsToKeep
        end
    end
    if headers == nil then
        headers = extractHeaders(inputTable)
    end

    -- newHeaders are needed if there are quotes within the header
    -- because they need to be escaped
    local newHeaders = {}
    for i = 1, #headers do
        if headers[i]:find('"') then
            newHeaders[i] = headers[i]:gsub('"', '""')
        else
            newHeaders[i] = headers[i]
        end
    end
    output[1] = '"' .. table.concat(newHeaders, '"' .. delimiter .. '"') .. '"\r\n'

    -- add each line by line.
    for i, line in writer(inputTable, delimiter, headers) do
        output[i+1] = line
    end
    return table.concat(output)
end

return ftcsv

