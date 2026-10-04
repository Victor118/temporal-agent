-- make_slides' pandoc filter.

-- A local image: a bare file name, one of the files the call copied next to
-- the document.
local function local_name(src)
  return src ~= "" and src ~= "." and src ~= ".." and not src:find("[/\\:]")
end

-- --sandbox lets the pptx writer read no file but the input: a local image
-- goes into the media bag first, read from the document's directory, where
-- nothing but the call's files lies. Anything else is left to the writer,
-- which refuses it.
function Image(img)
  if FORMAT ~= "pptx" or not local_name(img.src) or pandoc.mediabag.lookup(img.src) then
    return nil
  end
  local f = io.open(img.src, "rb")
  if f then
    local data = f:read("a")
    f:close()
    pandoc.mediabag.insert(img.src, nil, data)
  end
  return nil
end

-- Speaker notes stay out of a PDF; a pptx keeps them as notes.
function Div(div)
  if FORMAT == "typst" and div.classes:includes("notes") then
    return {}
  end
  return nil
end
