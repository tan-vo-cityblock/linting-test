--this is to get only members who are not enrolled in HH (taken from a google sheet list)
--https://docs.google.com/spreadsheets/d/1w2niYHtUS4KuLBH-CQkEnfpL-3Wr9kofeIPEJ-7x0vM/edit?ts=60db58cf#gid=1654321
select * from {{ ref('member') }}
where partnername = 'CareFirst'
and patientid not in ('0061dc9a-037d-11eb-b92d-42010a8e00cf',
'018da40a-1aca-11eb-9312-42010a8e00cf',
'01b75fcc-03a8-11eb-875a-42010a8e00cf',
'0521ed74-03a5-11eb-84e1-42010a8e00cf',
'05702302-038e-11eb-8cf3-42010a8e00cf',
'063833ee-037d-11eb-b92d-42010a8e00cf',
'065a8708-039d-11eb-9dea-42010a8e00cf',
'06f11cf0-038d-11eb-8cf3-42010a8e00cf',
'0761793e-039a-11eb-9d41-42010a8e00cf',
'07b40bd6-037c-11eb-a3cd-42010a8e00cf',
'07b9bf66-037e-11eb-a793-42010a8e00cf',
'08b607e2-03a8-11eb-875a-42010a8e00cf',
'08e587f0-037c-11eb-a3cd-42010a8e00cf',
'0900ef2e-037f-11eb-b3ac-42010a8e00cf',
'09300650-03a3-11eb-9dea-42010a8e00cf',
'099eef5c-0394-11eb-adc3-42010a8e00cf',
'09f2c99c-039e-11eb-9dea-42010a8e00cf',
'09ff5078-039d-11eb-9dea-42010a8e00cf',
'0c140b22-039a-11eb-9d41-42010a8e00cf',
'0c82691a-039e-11eb-9dea-42010a8e00cf',
'0d33be58-039a-11eb-9d41-42010a8e00cf',
'0de2cb40-03a5-11eb-84e1-42010a8e00cf',
'0e376574-039b-11eb-bdba-42010a8e00cf',
'0e3bb960-038e-11eb-8cf3-42010a8e00cf',
'0f526b72-03a3-11eb-9dea-42010a8e00cf',
'0ffee7ca-037c-11eb-a3cd-42010a8e00cf',
'128504bc-0380-11eb-b3ac-42010a8e00cf',
'12d57026-03a5-11eb-84e1-42010a8e00cf',
'13032c42-03a4-11eb-84e1-42010a8e00cf',
'13c36edc-039d-11eb-9dea-42010a8e00cf',
'152d6a18-037d-11eb-b92d-42010a8e00cf',
'162044ee-038d-11eb-8cf3-42010a8e00cf',
'162bc360-03a5-11eb-84e1-42010a8e00cf',
'16d93b4c-037f-11eb-b3ac-42010a8e00cf',
'17405978-03a5-11eb-84e1-42010a8e00cf',
'1792960e-037b-11eb-a99f-42010a8e00cf',
'198fc66e-039b-11eb-bdba-42010a8e00cf',
'1a802fa8-037f-11eb-b3ac-42010a8e00cf',
'1ac64368-038d-11eb-8cf3-42010a8e00cf',
'1c6d1c16-03a4-11eb-84e1-42010a8e00cf',
'1d0b6f3c-037d-11eb-b92d-42010a8e00cf',
'1d215c9a-0394-11eb-adc3-42010a8e00cf',
'1db06e34-03a9-11eb-875a-42010a8e00cf',
'1e20db94-037f-11eb-b3ac-42010a8e00cf',
'204b3fb2-039e-11eb-9dea-42010a8e00cf',
'217c1cd0-03a3-11eb-9dea-42010a8e00cf',
'21abbf92-03a5-11eb-84e1-42010a8e00cf',
'22df5c42-03a6-11eb-84e1-42010a8e00cf',
'255228b0-03a6-11eb-84e1-42010a8e00cf',
'26e14256-037e-11eb-a793-42010a8e00cf',
'274da30c-039b-11eb-bdba-42010a8e00cf',
'287b0bb0-038d-11eb-8cf3-42010a8e00cf',
'2934862c-1aca-11eb-bc76-42010a8e00cf',
'29f1428a-03a5-11eb-84e1-42010a8e00cf',
'2a5bb3b2-039c-11eb-9dea-42010a8e00cf',
'2b7cac70-1aca-11eb-b826-42010a8e00cf',
'2cddffae-03a2-11eb-9dea-42010a8e00cf',
'2dd5fdcc-03a6-11eb-84e1-42010a8e00cf',
'2e6924aa-03a4-11eb-84e1-42010a8e00cf',
'2f359352-03a7-11eb-84e1-42010a8e00cf',
'2f80c38e-03a4-11eb-84e1-42010a8e00cf',
'301819bc-03a6-11eb-84e1-42010a8e00cf',
'3041606a-037e-11eb-a793-42010a8e00cf',
'304fa952-0380-11eb-a127-42010a8e00cf',
'315938c0-1aca-11eb-9660-42010a8e00cf',
'31c257ee-039e-11eb-9dea-42010a8e00cf',
'33b55b5c-038d-11eb-8cf3-42010a8e00cf',
'34338fc4-03a4-11eb-84e1-42010a8e00cf',
'34a890f8-0381-11eb-8aa8-42010a8e00cf',
'34f0afce-03a8-11eb-875a-42010a8e00cf',
'3621bec6-03a6-11eb-84e1-42010a8e00cf',
'36476c18-039a-11eb-9d41-42010a8e00cf',
'366ece34-03a9-11eb-875a-42010a8e00cf',
'3718d0b2-038d-11eb-8cf3-42010a8e00cf',
'37fb4c88-037b-11eb-a99f-42010a8e00cf',
'382de6c4-0381-11eb-8aa8-42010a8e00cf',
'3a04a0fc-038e-11eb-8cf3-42010a8e00cf',
'3a63bb5c-1aca-11eb-81f2-42010a8e00cf',
'3b38d02c-037c-11eb-ba2a-42010a8e00cf',
'3b3ed810-039e-11eb-9dea-42010a8e00cf',
'3c0146f2-0380-11eb-a127-42010a8e00cf',
'3d494aba-038e-11eb-8cf3-42010a8e00cf',
'3e3cdd58-03a5-11eb-84e1-42010a8e00cf',
'3e8f69da-0394-11eb-adc3-42010a8e00cf',
'3fc3230a-0394-11eb-adc3-42010a8e00cf',
'4207b574-03a6-11eb-84e1-42010a8e00cf',
'43170ec4-6d73-11eb-9efc-42010a8e0140',
'455d17f8-0394-11eb-adc3-42010a8e00cf',
'4700683c-038d-11eb-8cf3-42010a8e00cf',
'478f31c2-0381-11eb-8aa8-42010a8e00cf',
'4831fa18-037e-11eb-a793-42010a8e00cf',
'4b72a552-039b-11eb-b75a-42010a8e00cf',
'4ba66184-03a6-11eb-84e1-42010a8e00cf',
'4c5f1494-039d-11eb-9dea-42010a8e00cf',
'4d3fe830-037e-11eb-a793-42010a8e00cf',
'4e88e258-0380-11eb-a127-42010a8e00cf',
'4e898818-6d73-11eb-9efc-42010a8e0140',
'500d1892-03a3-11eb-9dea-42010a8e00cf',
'50442038-039b-11eb-b75a-42010a8e00cf',
'5047d03c-039d-11eb-9dea-42010a8e00cf',
'512fbd92-03a3-11eb-9dea-42010a8e00cf',
'51fd5ea0-037b-11eb-a99f-42010a8e00cf',
'52184834-038d-11eb-8cf3-42010a8e00cf',
'537bd71a-037c-11eb-ba2a-42010a8e00cf',
'538cf85e-6d73-11eb-9efc-42010a8e0140',
'54860416-038e-11eb-8cf3-42010a8e00cf',
'548d4ae4-037c-11eb-ba2a-42010a8e00cf',
'5528c4ea-03a6-11eb-84e1-42010a8e00cf',
'56874974-039c-11eb-9dea-42010a8e00cf',
'57176724-03a7-11eb-84e1-42010a8e00cf',
'57b4f34e-0394-11eb-adc3-42010a8e00cf',
'57b8de6a-039d-11eb-9dea-42010a8e00cf',
'57d8b7fa-03a5-11eb-84e1-42010a8e00cf',
'580574fa-037f-11eb-b3ac-42010a8e00cf',
'586eab2e-6d73-11eb-9efc-42010a8e0140',
'5872622c-03a2-11eb-9dea-42010a8e00cf',
'58d5646c-038e-11eb-8cf3-42010a8e00cf',
'5a23d316-0394-11eb-adc3-42010a8e00cf',
'5d0a0004-03a4-11eb-84e1-42010a8e00cf',
'5e628566-037c-11eb-ba2a-42010a8e00cf',
'5f496440-039a-11eb-9d41-42010a8e00cf',
'5fe65a5a-039c-11eb-9dea-42010a8e00cf',
'5ff141c0-0380-11eb-a127-42010a8e00cf',
'60811bde-038e-11eb-8cf3-42010a8e00cf',
'6227063e-03a1-11eb-9dea-42010a8e00cf',
'622dfbf6-03a6-11eb-84e1-42010a8e00cf',
'62969b24-03a7-11eb-84e1-42010a8e00cf',
'62e1391e-038d-11eb-8cf3-42010a8e00cf',
'63ec3628-038e-11eb-8cf3-42010a8e00cf',
'652c0618-038d-11eb-8cf3-42010a8e00cf',
'65adf434-03a6-11eb-84e1-42010a8e00cf',
'66d93e20-0380-11eb-a127-42010a8e00cf',
'675a1854-03a5-11eb-84e1-42010a8e00cf',
'6763e59c-0381-11eb-8aa8-42010a8e00cf',
'68b25e1e-039b-11eb-9dea-42010a8e00cf',
'694b1188-0380-11eb-a127-42010a8e00cf',
'695ff4ba-03a6-11eb-84e1-42010a8e00cf',
'699608d4-039c-11eb-9dea-42010a8e00cf',
'6c30292c-03a5-11eb-84e1-42010a8e00cf',
'6d4c2790-0394-11eb-8134-42010a8e00cf',
'6d4c7e22-03a1-11eb-9dea-42010a8e00cf',
'6db0b39e-039a-11eb-9d41-42010a8e00cf',
'6df08894-03a3-11eb-ad64-42010a8e00cf',
'6e13b4b0-6d73-11eb-99fb-42010a8e0140',
'6ef85618-039e-11eb-9dea-42010a8e00cf',
'6f6b3faa-037d-11eb-a793-42010a8e00cf',
'6f95e8c8-037c-11eb-ba2a-42010a8e00cf',
'6f9e0cc2-039c-11eb-9dea-42010a8e00cf',
'72241bd0-03a1-11eb-9dea-42010a8e00cf',
'7334c74e-03a7-11eb-84e1-42010a8e00cf',
'733d85ec-6d73-11eb-99fb-42010a8e0140',
'73553048-039c-11eb-9dea-42010a8e00cf',
'751c2654-039a-11eb-9d41-42010a8e00cf',
'76d0a7b4-037b-11eb-a99f-42010a8e00cf',
'772ecc08-0381-11eb-8aa8-42010a8e00cf',
'77b3b182-03a6-11eb-84e1-42010a8e00cf',
'77bbebba-03a4-11eb-84e1-42010a8e00cf',
'77cc679e-03a7-11eb-84e1-42010a8e00cf',
'7852a0d2-0381-11eb-8aa8-42010a8e00cf',
'78964d9c-03a3-11eb-9fa9-42010a8e00cf',
'79078e12-037b-11eb-a99f-42010a8e00cf',
'7a68a172-03a5-11eb-84e1-42010a8e00cf',
'7ab5802e-0381-11eb-8aa8-42010a8e00cf',
'7c1db51c-039a-11eb-9d41-42010a8e00cf',
'7c96e860-03a4-11eb-84e1-42010a8e00cf',
'7ccf8c8c-03a5-11eb-84e1-42010a8e00cf',
'7cd030be-03a1-11eb-9dea-42010a8e00cf',
'7e60b60e-0394-11eb-a616-42010a8e00cf',
'7f41e94c-03a5-11eb-84e1-42010a8e00cf',
'7f682f14-0394-11eb-a616-42010a8e00cf',
'7fa00fe8-037f-11eb-b3ac-42010a8e00cf',
'80202080-037e-11eb-86e7-42010a8e00cf',
'817a3d42-037b-11eb-a99f-42010a8e00cf',
'82afa1ae-039c-11eb-9dea-42010a8e00cf',
'82c771e0-03a5-11eb-84e1-42010a8e00cf',
'8347a2b2-037c-11eb-b92d-42010a8e00cf',
'84a14884-03a4-11eb-84e1-42010a8e00cf',
'877f5d24-03a5-11eb-84e1-42010a8e00cf',
'88137dc6-039e-11eb-9dea-42010a8e00cf',
'89c38a50-037e-11eb-86e7-42010a8e00cf',
'8a72e844-039a-11eb-9d41-42010a8e00cf',
'8ae9a73e-037e-11eb-86e7-42010a8e00cf',
'8b85943a-039e-11eb-9dea-42010a8e00cf',
'8c4bd48a-03a1-11eb-9dea-42010a8e00cf',
'8e683452-038d-11eb-8cf3-42010a8e00cf',
'8f1f8bac-03a6-11eb-84e1-42010a8e00cf',
'8f8b996e-037e-11eb-86e7-42010a8e00cf',
'8fc20f66-039d-11eb-9dea-42010a8e00cf',
'8ff058c6-03a7-11eb-84e1-42010a8e00cf',
'9061228a-0394-11eb-a616-42010a8e00cf',
'91cc9c28-038d-11eb-8cf3-42010a8e00cf',
'925aac54-0381-11eb-8aa8-42010a8e00cf',
'92b992a0-039a-11eb-9d41-42010a8e00cf',
'9346d2ce-03a3-11eb-9fa9-42010a8e00cf',
'93ca3990-037e-11eb-86e7-42010a8e00cf',
'94944cd8-03a3-11eb-9fa9-42010a8e00cf',
'959f656c-03a4-11eb-84e1-42010a8e00cf',
'95da1198-03a5-11eb-84e1-42010a8e00cf',
'9617c046-03a6-11eb-84e1-42010a8e00cf',
'965debae-039a-11eb-9d41-42010a8e00cf',
'983e52d4-03a8-11eb-875a-42010a8e00cf',
'984e4f9e-037c-11eb-b92d-42010a8e00cf',
'993c72da-039c-11eb-9dea-42010a8e00cf',
'99627bc6-037c-11eb-b92d-42010a8e00cf',
'997d7a04-037f-11eb-b3ac-42010a8e00cf',
'9a41fa6e-03a2-11eb-9dea-42010a8e00cf',
'9c333ddc-0394-11eb-a616-42010a8e00cf',
'9d0e4418-03a3-11eb-9fa9-42010a8e00cf',
'9fa3904a-037e-11eb-86e7-42010a8e00cf',
'a07bca08-0394-11eb-a616-42010a8e00cf',
'a1ea565e-037e-11eb-86e7-42010a8e00cf',
'a34cb304-0381-11eb-8aa8-42010a8e00cf',
'a4fe7b68-03a6-11eb-84e1-42010a8e00cf',
'a5e72902-037f-11eb-b3ac-42010a8e00cf',
'a6a243ce-039b-11eb-9dea-42010a8e00cf',
'a7076a54-03a7-11eb-84e1-42010a8e00cf',
'a722ce8e-037f-11eb-b3ac-42010a8e00cf',
'a81d814a-037e-11eb-86e7-42010a8e00cf',
'a8d430dc-037b-11eb-a99f-42010a8e00cf',
'a98dc28c-037f-11eb-b3ac-42010a8e00cf',
'a9a41e7e-03a2-11eb-9dea-42010a8e00cf',
'aa47737e-03a4-11eb-84e1-42010a8e00cf',
'abcf2ae0-03a7-11eb-875a-42010a8e00cf',
'ac459e24-038d-11eb-8cf3-42010a8e00cf',
'acecd396-03a7-11eb-875a-42010a8e00cf',
'ad53055e-03a6-11eb-84e1-42010a8e00cf',
'ad5ea86a-c3b2-11eb-87aa-42010a8e002b',
'adb428aa-03a8-11eb-875a-42010a8e00cf',
'ae6e6f0e-037f-11eb-b3ac-42010a8e00cf',
'affa1450-a158-11eb-93b0-42010a8e0073',
'b14b2f90-0380-11eb-a127-42010a8e00cf',
'b15f38a6-03a7-11eb-875a-42010a8e00cf',
'b1fab02e-039d-11eb-9dea-42010a8e00cf',
'b2299e8c-03a4-11eb-84e1-42010a8e00cf',
'b5fd3ea8-03a7-11eb-875a-42010a8e00cf',
'b6ce50f4-03a4-11eb-84e1-42010a8e00cf',
'b855b578-03a6-11eb-84e1-42010a8e00cf',
'ba12001c-039a-11eb-bdba-42010a8e00cf',
'ba5fe43a-03a4-11eb-84e1-42010a8e00cf',
'ba62bd24-03a2-11eb-9dea-42010a8e00cf',
'bb142ada-0381-11eb-b8fa-42010a8e00cf',
'bb77c0a6-037f-11eb-b3ac-42010a8e00cf',
'bbdf7ea4-03a6-11eb-84e1-42010a8e00cf',
'bc5c5ff2-039a-11eb-bdba-42010a8e00cf',
'bc83e0ea-0381-11eb-b8fa-42010a8e00cf',
'bcd01502-039d-11eb-9dea-42010a8e00cf',
'c13e30a0-03a3-11eb-b9a5-42010a8e00cf',
'c2b2cb8e-037c-11eb-b92d-42010a8e00cf',
'c3d5746e-039d-11eb-9dea-42010a8e00cf',
'c3f74f30-03a2-11eb-9dea-42010a8e00cf',
'c4bef36c-039a-11eb-bdba-42010a8e00cf',
'c87b9078-037c-11eb-b92d-42010a8e00cf',
'c9810a46-039c-11eb-9dea-42010a8e00cf',
'c98d3ade-039a-11eb-bdba-42010a8e00cf',
'c9b4d202-03a3-11eb-84e1-42010a8e00cf',
'cabad29c-03a7-11eb-875a-42010a8e00cf',
'cf26218a-039b-11eb-9dea-42010a8e00cf',
'cf465f4e-037e-11eb-b3ac-42010a8e00cf',
'd0f75838-039d-11eb-9dea-42010a8e00cf',
'd15891ec-0399-11eb-9d41-42010a8e00cf',
'd192b558-03a2-11eb-9dea-42010a8e00cf',
'd1faddf0-03a1-11eb-9dea-42010a8e00cf',
'd237a824-039d-11eb-9dea-42010a8e00cf',
'd2607e88-0380-11eb-a127-42010a8e00cf',
'd2b81ad8-037d-11eb-a793-42010a8e00cf',
'd3edc350-0399-11eb-9d41-42010a8e00cf',
'd5aa0b88-039c-11eb-9dea-42010a8e00cf',
'd8184bf2-039a-11eb-bdba-42010a8e00cf',
'd8efbe9e-0399-11eb-9d41-42010a8e00cf',
'd910c414-03a7-11eb-875a-42010a8e00cf',
'daed795e-03a6-11eb-84e1-42010a8e00cf',
'db40788e-038d-11eb-8cf3-42010a8e00cf',
'dc91fd94-037d-11eb-a793-42010a8e00cf',
'dd9cf880-037b-11eb-8dfd-42010a8e00cf',
'dfb94c96-037f-11eb-b3ac-42010a8e00cf',
'e049503e-03a7-11eb-875a-42010a8e00cf',
'e10de034-039d-11eb-9dea-42010a8e00cf',
'e2dbbbbc-038d-11eb-8cf3-42010a8e00cf',
'e308c6fc-03a6-11eb-84e1-42010a8e00cf',
'e3d897dc-03a7-11eb-875a-42010a8e00cf',
'e4d570ec-03a2-11eb-9dea-42010a8e00cf',
'e50b177a-03a1-11eb-9dea-42010a8e00cf',
'e578dc68-03a3-11eb-84e1-42010a8e00cf',
'e5fad840-0393-11eb-bcbf-42010a8e00cf',
'e627fab0-038d-11eb-8cf3-42010a8e00cf',
'e976e51e-038d-11eb-8cf3-42010a8e00cf',
'e980a454-03a2-11eb-9dea-42010a8e00cf',
'ea432490-037d-11eb-a793-42010a8e00cf',
'eaaa95b0-03a2-11eb-9dea-42010a8e00cf',
'ebb0276c-037b-11eb-8dfd-42010a8e00cf',
'ee1bbf30-03a2-11eb-9dea-42010a8e00cf',
'ee1fb102-037b-11eb-8dfd-42010a8e00cf',
'ef824c94-0380-11eb-a127-42010a8e00cf',
'f0045e9e-039b-11eb-9dea-42010a8e00cf',
'f147a40e-03a6-11eb-84e1-42010a8e00cf',
'f18e5fce-039d-11eb-9dea-42010a8e00cf',
'f1992a0c-037b-11eb-8dfd-42010a8e00cf',
'f284ea58-8758-11eb-98d0-42010a8e00d5',
'f32d7c08-039c-11eb-9dea-42010a8e00cf',
'f3405b60-03a7-11eb-875a-42010a8e00cf',
'f45fdda4-03a7-11eb-875a-42010a8e00cf',
'f4bb6686-03a4-11eb-84e1-42010a8e00cf',
'f5389090-0393-11eb-adc3-42010a8e00cf',
'f58b5d9e-039c-11eb-9dea-42010a8e00cf',
'f5d241b6-03a4-11eb-84e1-42010a8e00cf',
'f6d8a594-038d-11eb-8cf3-42010a8e00cf',
'f8334a4e-03a5-11eb-84e1-42010a8e00cf',
'f85c3a80-039b-11eb-9dea-42010a8e00cf',
'f8b77eec-0380-11eb-9268-42010a8e00cf',
'f989e988-039c-11eb-9dea-42010a8e00cf',
'fb004698-0380-11eb-96d8-42010a8e00cf',
'fb239248-0393-11eb-adc3-42010a8e00cf',
'fc235b82-0380-11eb-9658-42010a8e00cf',
'fc42fa14-03a3-11eb-84e1-42010a8e00cf',
'fd89a25e-039b-11eb-9dea-42010a8e00cf',
'fd947e1c-038d-11eb-8cf3-42010a8e00cf',
'ff2172e6-037c-11eb-b92d-42010a8e00cf',
'ff2d87ec-8758-11eb-b708-42010a8e00d5',
'ff3cc322-03a7-11eb-875a-42010a8e00cf')
