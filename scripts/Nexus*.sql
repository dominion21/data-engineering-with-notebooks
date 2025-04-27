BEGIN
  FOR file_name IN
    (SELECT VALUE AS file_name
     FROM TABLE(
       DIRECTORY_LIST('@DEMO_GIT_REPO/branches/dev/scripts/')
     )
     WHERE file_name LIKE 'Nexus%.sql')
  DO
    EXECUTE IMMEDIATE FROM file_name;
  END FOR;
END;
