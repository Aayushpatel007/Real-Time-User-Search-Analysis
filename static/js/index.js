function AsHandler() {
  document.getElementById("btn-sbmt").classList.toggle("hide-show");
}

function getvalHandler() {
  let xx = document.getElementById("search").value;
  document.getElementById("search1").value = xx;
  console.log(document.getElementById("search").value);
}
