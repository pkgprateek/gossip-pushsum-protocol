#time "on"
// Fetching libraries from nuget
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"

// Inititalizing Libraries
open Akka
open Akka.FSharp
open Akka.Actor
open System
open System.Diagnostics

// Creating a Discriminated union to pass participant pool list
type info =
| Rumour of (int array * string * int)
| Arguments of (int * string * string)
| PushSum of (float * float * int array * int)
| Converged of (string)
| Numnodes of (int)

// Actor Configuration
let system = System.create "gossip" <| Configuration.load()

let estimate : double = (10.0)**(-10.0)

// Reference for topology from command line
let mutable topRef = ""

// Full Topology
let mutable participantenum = [||]

// Full Topology
let mutable fullTopo = [||]

// 3D Topology
let mutable threeDTopo = [||]

// 3D Topology
let mutable ImpthreeDTopo = [||]

// Line Topology
let mutable lineTopo = [||]

// Creating timer
let timer = Stopwatch()


// Random function
let randomNeighbour (n: int[]) = 
    let r = Random().Next(n.Length)       
    if topRef = "3d" then
        n.[r], threeDTopo.[n.[r]]
    elif topRef = "line" then
        n.[r], lineTopo.[n.[r]]
    elif topRef = "imp3d" then
        n.[r], ImpthreeDTopo.[n.[r]]
    else
        n.[r], (Array.filter (fun elem -> elem <> n.[r]) fullTopo)

//Convergence actor
let Convergence (mailbox:Actor<_>) = 

    let state = ref 0
    let mutable totalNodes = 0

    let rec messageloop() = actor {

            let! message = mailbox.Receive()
            match message with 
            | Converged message ->
                let timeElapsed  = double timer.ElapsedMilliseconds
                //printfn "Elapsed time :%f" timeElapsed
                timer.Stop()
                mailbox.Context.System.Terminate() |> ignore
                //Environment.Exit 0   
            | _->()

            return! messageloop()
        }
    messageloop()   
let converger = spawn system "converger" Convergence


// Participant Actor
let participant (mailbox : Actor<_>) =
    
    // Creating variable to check the number of times the rumour was heard by our actor
    let mutable rumourCount = 0

    let rec messageLoop () = actor {    
        // Reading the message
        let! msg = mailbox.Receive()
        match msg with
        | Rumour(myNeighbours, rumour, c) ->
            rumourCount <- rumourCount + c
            if rumourCount < 10 then
                let selectRandom, newNeighbours = randomNeighbour myNeighbours
                participantenum.[selectRandom] <! Rumour(newNeighbours, rumour, 1)
                let selectRandom, newNeighbours = randomNeighbour myNeighbours
                participantenum.[selectRandom] <! Rumour(newNeighbours, rumour, 1)
                mailbox.Self <! Rumour(myNeighbours, rumour, 0)
            else
                mailbox.Context.System.Terminate() |> ignore
                converger <! Converged("Done")
                
        |_-> ()
 
        return! messageLoop()
    }
    messageLoop ()

let pushsumParticipant (mailbox: Actor<_>) =
    
    let mutable s = 0.0
    let mutable w = 0.0
    let mutable sNew = 0.0
    let mutable wNew = 0.0
    let mutable ratioOld = 0.0
    let mutable ratioNew = 0.0
    let mutable diff : double = 0.0
    let mutable pushsumCount = 0

    let rec messageLoop () = 
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | PushSum(xi, wi, myNeighbours, c) ->
                if c = 1 then
                    sNew <- s + xi
                    wNew <- w + wi
                    ratioNew <- sNew / wNew
                    diff <- ratioNew - ratioOld
                    if pushsumCount = 3 then
                        mailbox.Context.System.Terminate() |> ignore
                        converger <! Converged("Done")
                    elif diff <= estimate then
                        pushsumCount <- pushsumCount + 1
                    else
                        s <- sNew
                        w <- wNew
                        ratioOld <- s / w
                        let selectRandom, newNeighbours = randomNeighbour myNeighbours
                        participantenum.[selectRandom] <! PushSum(s/2.0, w/2.0, newNeighbours, 1)
                        mailbox.Self <! PushSum(s, w, myNeighbours, 0)
                else
                    let selectRandom, newNeighbours = randomNeighbour myNeighbours
                    participantenum.[selectRandom] <! PushSum(s / 2.0, w / 2.0, newNeighbours, 1)
                    mailbox.Self <! PushSum(s, w, myNeighbours, 0)
            |_ -> ()
            return! messageLoop()
        }
    messageLoop ()
    
    // Main Actor
let mainActor (mailbox : Actor<_>) = 
    
    // Specifying Rumour
    let rumour = "DOS project 2 has many rumours."

    let mutable counter = 0
    let rec messageLoop () = 
        actor {
                let! msg = mailbox.Receive()

                match msg with
                | Arguments(numNodes,topology,algorithm)->
               
                    // Creating a Participant Pool
                    if algorithm = "gossip" then
                        let participantPool = 
                            [0 .. numNodes-1]
                                |> List.map(fun i -> spawn system (sprintf "Participant_%d" i) participant )
                        participantenum <- Array.ofList(participantPool)
                    if algorithm = "pushsum" then
                        let participantPool = 
                            [0 .. numNodes-1]
                                |> List.map(fun i -> spawn system (sprintf "Participant_%d" i) pushsumParticipant )
                        participantenum <- Array.ofList(participantPool)
                | _-> printfn "I am not what you expect"
                return! messageLoop ()
            }
    messageLoop ()

//Take commandline input
let args : string array = fsi.CommandLineArgs |> Array.tail
let mutable numNodes = args.[0] |> int
let topology = args.[1] |> string
let algorithm=args.[2] |>string
topRef <- topology

// Spawning the Actor System
let initiater = spawn system "main" mainActor
initiater <! Arguments(numNodes,topology,algorithm)
converger <! Numnodes(numNodes)


// Wait till system is terminated
system.WhenTerminated.Wait()
